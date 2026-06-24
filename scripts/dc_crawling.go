package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gocolly/colly"
	"github.com/xuri/excelize/v2"
)

type PostData struct {
	CollectionTime string
	Nick           string
	UIDIP          string
	PostNum        int
	ComNum         int
	isIP           string
}

type UserData struct {
	Nickname string `json:"nickname"`
	ID       string `json:"id"`
	Type     string `json:"type"`
	Post     int    `json:"post"`
	Comment  int    `json:"comment"`
	Total    int    `json:"total"`
}

type Comment struct {
	UserID     string `json:"user_id"`
	Name       string `json:"name"`
	IP         string `json:"ip"`
	RegDate    string `json:"reg_date"`
	GallogIcon string `json:"gallog_icon"`
	Memo       string `json:"memo"`
}

type ResponseData struct {
	Comments []Comment `json:"comments"`
}

type CheckpointData struct {
	RemainingPosts []int                `json:"remaining_posts"`
	SavedData      map[string]*PostData `json:"saved_data"`
}

var (
	kstLoc       *time.Location
	dataMap      = make(map[string]*PostData)
	mapMutex     sync.Mutex
	sharedClient = &http.Client{
		Timeout: 20 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 50,
			IdleConnTimeout:     90 * time.Second,
		},
	}
	globalEsno      string
	globalEsnoMutex sync.RWMutex
	esnoRegex       = regexp.MustCompile(`<input[^>]+id="e_s_n_o"[^>]+value="([^"]+)"`)
)

var userAgents = []string{
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
}

var (
	banMutex sync.Mutex
	banUntil time.Time
	logMutex sync.Mutex // 💡 로그 파일 동시성 제어용
)

// 💡 시스템 전용 통합 로그 작성기 (Print를 대체하고 R2에 실시간 업로드)
func sysLog(level, format string, args ...interface{}) {
	logMutex.Lock()
	defer logMutex.Unlock()

	msg := fmt.Sprintf("[%s] [%s] %s\n", time.Now().In(kstLoc).Format("15:04:05"), level, fmt.Sprintf(format, args...))
	
	f, err := os.OpenFile("crawler_debug_log.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err == nil {
		f.WriteString(msg)
		f.Close()
	}

	// 에러나 중요한 시스템 단계일 경우 R2에 즉시 덮어쓰기 (업로드)
	if level == "ERROR" || level == "CRITICAL" || level == "SYSTEM" {
		uploadToR2("crawler_debug_log.txt", "crawler_debug_log.txt")
	}
}

func checkBanState() {
	banMutex.Lock()
	if time.Now().Before(banUntil) {
		sleepDur := time.Until(banUntil)
		banMutex.Unlock()
		time.Sleep(sleepDur)
		return
	}
	banMutex.Unlock()
}

func triggerBan(source string) {
	banMutex.Lock()
	defer banMutex.Unlock()
	if time.Now().After(banUntil) {
		sysLog("ERROR", "🚨 서버 차단(403/503) 감지! 2분간 모든 스레드 대기 상태 돌입 (발생지점: %s)", source)
		banUntil = time.Now().Add(2 * time.Minute)
	}
}

func getRandomUA() string {
	return userAgents[rand.Intn(len(userAgents))]
}

func init() {
	var err error
	kstLoc, err = time.LoadLocation("Asia/Seoul")
	if err != nil {
		kstLoc = time.FixedZone("KST", 9*60*60)
	}
	rand.Seed(time.Now().UnixNano())
}

func updateMemory(collectionTime string, nick string, uid string, isPost bool, isIp string) {
	mapMutex.Lock()
	defer mapMutex.Unlock()

	if _, exists := dataMap[uid]; !exists {
		dataMap[uid] = &PostData{
			CollectionTime: collectionTime,
			Nick:           nick,
			UIDIP:          uid,
			isIP:           isIp,
		}
	}
	entry := dataMap[uid]
	if nick != "" {
		entry.Nick = nick
	}
	if isPost {
		entry.PostNum++
	} else {
		entry.ComNum++
	}
}

func findTargetHourPosts(targetStart, targetEnd time.Time) ([]int, string, string, error) {
	c := colly.NewCollector(colly.UserAgent(getRandomUA()))
	c.SetRequestTimeout(30 * time.Second)

	var validPostNumbers []int
	var startDate, endDate string
	page := 1
	done := false
	visitedIDs := make(map[int]bool)
	consecutiveOldPosts := 0
	const maxConsecutiveOld = 15

	var networkErr error
	var postsOnPage int          

	c.OnError(func(r *colly.Response, err error) {
		sysLog("ERROR", "게시글 목록 탐색 중 오류 발생 (URL: %s, 상태: %d, 에러: %v)", r.Request.URL, r.StatusCode, err)
		if r.StatusCode == 403 || r.StatusCode == 503 {
			networkErr = fmt.Errorf("디시인사이드 서버 목록 밴 (HTTP %d)", r.StatusCode)
			done = true 
		}
	})

	c.OnHTML("tr.ub-content", func(e *colly.HTMLElement) {
		if done { return }
		postsOnPage++

		if _, err := strconv.Atoi(e.ChildText("td.gall_num")); err != nil { return }
		subject := strings.TrimSpace(e.ChildText("td.gall_subject"))
		if subject == "설문" || subject == "AD" || subject == "공지" { return }

		noStr := e.Attr("data-no")
		postNo, err := strconv.Atoi(noStr)
		if err != nil { return }

		if visitedIDs[postNo] { return }
		visitedIDs[postNo] = true

		title := e.ChildAttr("td.gall_date", "title")
		if title == "" { title = strings.TrimSpace(e.ChildText("td.gall_date")) }

		var postTime time.Time
		postTime, _ = time.ParseInLocation("2006-01-02 15:04:05", title, kstLoc)

		// ... (이전과 동일한 시간 보정 로직 생략 없이 탑재됨) ...
		if postTime.IsZero() {
			if strings.Contains(title, ":") && !strings.Contains(title, "-") {
				todayStr := time.Now().In(kstLoc).Format("2006-01-02 ") + title + ":00"
				postTime, _ = time.ParseInLocation("2006-01-02 15:04:05", todayStr, kstLoc)
				if postTime.After(time.Now().In(kstLoc)) { postTime = postTime.Add(-24 * time.Hour) }
			}
		}

		if (postTime.Equal(targetStart) || postTime.After(targetStart)) && postTime.Before(targetEnd) {
			consecutiveOldPosts = 0
			validPostNumbers = append(validPostNumbers, postNo)
			if endDate == "" { endDate = title }
			startDate = title
		}

		if postTime.Before(targetStart) {
			consecutiveOldPosts++
			if consecutiveOldPosts >= maxConsecutiveOld { done = true }
		} else if postTime.After(targetEnd) || postTime.Equal(targetEnd) { 
			consecutiveOldPosts = 0 
		}
	})

	for !done {
		postsOnPage = 0 
		pageURL := fmt.Sprintf("https://gall.dcinside.com/mgallery/board/lists/?id=projectmx&page=%d", page)
		time.Sleep(time.Duration(1500+rand.Intn(2000)) * time.Millisecond) 
		
		c.Visit(pageURL)

		if networkErr != nil { return nil, "", "", networkErr }
		if postsOnPage == 0 {
			sysLog("CRITICAL", "목록 페이지 %d에서 게시글이 하나도 보이지 않습니다. 캡차 또는 섀도우 밴에 걸렸습니다.", page)
			return nil, "", "", fmt.Errorf("게시글 로딩 실패 (섀도우 밴 확실시)")
		}
		if page > 5000 { break }
		page++
	}

	return validPostNumbers, startDate, endDate, nil
}

func scrapePostsAndComments(validPosts []int, collectionTimeStr string, targetStart, targetEnd time.Time) error {
	c := colly.NewCollector(colly.UserAgent(getRandomUA()), colly.Async(true))
	c.SetRequestTimeout(60 * time.Second)
	
	c.Limit(&colly.LimitRule{
		DomainGlob:  "*",
		Parallelism: 2,               
		Delay:       4 * time.Second, 
		RandomDelay: 2 * time.Second, 
	})

	var visitedPosts sync.Map
	var failCount int32

	c.OnError(func(r *colly.Response, err error) {
		if r.StatusCode == 404 { return } 

		retries, _ := strconv.Atoi(r.Ctx.Get("retry_count"))
		sysLog("ERROR", "본문 수집 에러 발생! URL: %s, 상태: %d (시도 횟수: %d)", r.Request.URL.String(), r.StatusCode, retries)

		if r.StatusCode == 403 || r.StatusCode == 503 {
			triggerBan("본문 수집기") 
			if retries < 3 { 
				r.Ctx.Put("retry_count", strconv.Itoa(retries+1))
				r.Request.Retry() 
				return
			} else {
				atomic.AddInt32(&failCount, 1)
				return
			}
		}

		if r.StatusCode >= 500 || r.StatusCode == 0 {
			if retries < 3 {
				r.Ctx.Put("retry_count", strconv.Itoa(retries+1))
				time.Sleep(3 * time.Second)
				r.Request.Retry()
			} else {
				atomic.AddInt32(&failCount, 1)
			}
		}
	})

	c.OnRequest(func(r *colly.Request) {
		checkBanState() 
		r.Headers.Set("Referer", "https://gall.dcinside.com/mgallery/board/lists/?id=projectmx")
	})

	c.OnResponse(func(r *colly.Response) {
		matches := esnoRegex.FindSubmatch(r.Body)
		if len(matches) > 1 {
			parsedEsno := string(matches[1])
			r.Ctx.Put("esno", parsedEsno)
			globalEsnoMutex.Lock()
			globalEsno = parsedEsno
			globalEsnoMutex.Unlock()
		}
	})

	c.OnHTML("div.view_content_wrap", func(e *colly.HTMLElement) {
		noStr := e.Request.URL.Query().Get("no")
		no, err := strconv.Atoi(noStr)
		if err != nil { return }

		if _, loaded := visitedPosts.LoadOrStore(no, true); loaded { return }

		nick := e.ChildAttr(".gall_writer", "data-nick")
		uid := e.ChildAttr(".gall_writer", "data-uid")

		isip := ""
		if uid == "" {
			uid = e.ChildAttr(".gall_writer", "data-ip")
			isip = "유동"
		} else {
			if strings.Contains(e.ChildAttr(".gall_writer .writer_nikcon img", "src"), "fix_nik.gif") {
				isip = "고닉"
			} else {
				isip = "반고닉"
			}
		}

		postDateStr := e.ChildAttr(".gall_date", "title")
		if postDateStr == "" { postDateStr = e.ChildText(".gall_date") }
		pTime, err := time.ParseInLocation("2006-01-02 15:04:05", postDateStr, kstLoc)

		if err == nil && (pTime.Equal(targetStart) || pTime.After(targetStart)) && pTime.Before(targetEnd) {
			updateMemory(collectionTimeStr, nick, uid, true, isip)
		}

		esno := e.Request.Ctx.Get("esno")
		if esno == "" {
			globalEsnoMutex.RLock()
			esno = globalEsno
			globalEsnoMutex.RUnlock()
		}

		// ⚡ [성능 최적화 복구] HTML 상에서 댓글 개수가 0개면 API 통신 없이 즉시 패스합니다.
		commentCountStr := strings.TrimSpace(e.ChildText(".gall_comment .font_red"))
		if commentCountStr == "" || commentCountStr == "0" || commentCountStr == "[0]" {
			return 
		}

		commentSrc(no, esno, collectionTimeStr, targetStart, targetEnd)
	})

	for _, no := range validPosts {
		url := fmt.Sprintf("https://gall.dcinside.com/mgallery/board/view/?id=projectmx&no=%d", no)
		c.Visit(url)
	}
	
	c.Wait() 

	finalFailCount := atomic.LoadInt32(&failCount)
	if finalFailCount > 15 {
		sysLog("CRITICAL", "본문 수집 중 허용 한도 이상의 실패가 발생하여 해당 청크의 크롤링을 포기합니다. (누적 실패 수: %d)", finalFailCount)
		return fmt.Errorf("수집 실패 과다 (데이터 누수)")
	}
	return nil
}

func commentSrc(no int, esno string, collectionTimeStr string, targetStart, targetEnd time.Time) {
	if esno == "" {
		pageURL := fmt.Sprintf("https://gall.dcinside.com/mgallery/board/view/?id=projectmx&no=%d&t=cv", no)
		req, err := http.NewRequest("GET", pageURL, nil)
		if err != nil { return }
		req.Header.Set("User-Agent", getRandomUA())
		req.Header.Set("Referer", "https://gall.dcinside.com/")
		resp, err := sharedClient.Do(req)
		if err == nil {
			doc, err := goquery.NewDocumentFromReader(resp.Body)
			if err == nil && doc != nil {
				esno, _ = doc.Find("input#e_s_n_o").Attr("value")
				if esno != "" {
					globalEsnoMutex.Lock()
					globalEsno = esno
					globalEsnoMutex.Unlock()
				}
			}
			resp.Body.Close()
		}
	}
	if esno == "" { 
		sysLog("ERROR", "게시글 %d의 esno 토큰을 구하지 못해 댓글 수집을 포기합니다.", no)
		return 
	}

	endpoint := "https://gall.dcinside.com/board/comment/"
	sno := strconv.Itoa(no)
	headerurl := fmt.Sprintf("https://gall.dcinside.com/mgallery/board/view/?id=projectmx&no=%d&t=cv", no)

	page := 1
	for {
		var body []byte
		var reqErr error

		for retry := 0; retry < 3; retry++ {
			checkBanState() 

			data := url.Values{}
			data.Set("id", "projectmx")
			data.Set("no", sno)
			data.Set("cmt_id", "projectmx")
			data.Set("cmt_no", sno)
			data.Set("e_s_n_o", esno)
			data.Set("_GALLTYPE_", "M")
			data.Set("page", strconv.Itoa(page))

			req, _ := http.NewRequest("POST", endpoint, strings.NewReader(data.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.Header.Set("Referer", headerurl)
			req.Header.Set("User-Agent", getRandomUA())
			req.Header.Set("X-Requested-With", "XMLHttpRequest")

			resp, err := sharedClient.Do(req)
			if err == nil {
				if resp.StatusCode == 403 || resp.StatusCode == 503 {
					sysLog("ERROR", "게시글 %d 댓글 API 호출 중 차단(상태 %d) 발생. 재시도합니다.", no, resp.StatusCode)
					triggerBan("댓글 수집기 API") 
					resp.Body.Close()
					continue 
				}

				body, reqErr = io.ReadAll(resp.Body)
				resp.Body.Close()
				if reqErr == nil && len(body) > 0 { break }
			}
			time.Sleep(500 * time.Millisecond)
		}

		if len(body) == 0 { 
			sysLog("ERROR", "게시글 %d의 댓글 데이터를 가져오지 못했습니다. (서버 무응답)", no)
			break 
		}

		var responseData ResponseData
		if err := json.Unmarshal(body, &responseData); err != nil {
			sysLog("ERROR", "게시글 %d 댓글 JSON 파싱 실패: %v (받아온 데이터: %s)", no, err, string(body))
			break 
		}
		if len(responseData.Comments) == 0 { break }

		for _, comment := range responseData.Comments {
			if strings.TrimSpace(comment.Name) == "댓글돌이" { continue }
			if strings.Contains(comment.Memo, "삭제된 댓글입니다") && comment.UserID == "" { continue }

			var cTime time.Time
			var parseErr error
			reg := comment.RegDate

			if !strings.Contains(reg, ".") {
				reg = targetStart.Format("2006.01.02 ") + reg
			} else if strings.Count(reg, ".") == 1 {
				reg = fmt.Sprintf("%d.", targetStart.Year()) + reg
			}
			if strings.Count(reg, ":") == 1 { reg += ":00" }

			cTime, parseErr = time.ParseInLocation("2006.01.02 15:04:05", reg, kstLoc)

			if parseErr == nil {
				if cTime.Before(targetStart) || cTime.After(targetEnd) || cTime.Equal(targetEnd) { continue }
			} else { continue }

			isip := ""
			uniqueKey := comment.UserID
			if comment.UserID == "" {
				isip = "유동"
				uniqueKey = comment.IP
			} else {
				if strings.Contains(comment.GallogIcon, "fix_nik.gif") {
					isip = "고닉"
				} else {
					isip = "반고닉"
				}
			}
			updateMemory(collectionTimeStr, comment.Name, uniqueKey, false, isip)
		}
		page++
		time.Sleep(time.Duration(600+rand.Intn(400)) * time.Millisecond)
	}
}

func saveCheckpointLocal(filename string, cp *CheckpointData) error {
	file, err := os.Create(filename)
	if err != nil { return err }
	defer file.Close()
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(cp)
}

func saveJsonLocal(filename string) error {
	var jsonData []UserData
	for _, post := range dataMap {
		jsonData = append(jsonData, UserData{
			Nickname: post.Nick,
			ID:       post.UIDIP,
			Type:     post.isIP,
			Post:     post.PostNum,
			Comment:  post.ComNum,
			Total:    post.PostNum + post.ComNum,
		})
	}
	file, err := os.Create(filename)
	if err != nil { return err }
	defer file.Close()
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(jsonData)
}

func saveExcelLocal(filename string) error {
	f := excelize.NewFile()
	sheetName := "Sheet1"
	f.SetSheetName(f.GetSheetName(0), sheetName)
	customColumns := []string{"수집시간", "닉네임", "ID(IP)", "유저타입", "작성글수", "작성댓글수", "총활동수"}
	style, _ := f.NewStyle(&excelize.Style{
		Font: &excelize.Font{Bold: true},
		Fill: excelize.Fill{Type: "pattern", Color: []string{"#E0E0E0"}, Pattern: 1},
	})
	for i, colName := range customColumns {
		cell := fmt.Sprintf("%s%d", string(rune('A'+i)), 1)
		f.SetCellValue(sheetName, cell, colName)
		f.SetCellStyle(sheetName, cell, cell, style)
	}
	rowIndex := 2
	for _, post := range dataMap {
		totalActivity := post.PostNum + post.ComNum
		f.SetCellValue(sheetName, fmt.Sprintf("A%d", rowIndex), post.CollectionTime)
		f.SetCellValue(sheetName, fmt.Sprintf("B%d", rowIndex), post.Nick)
		f.SetCellValue(sheetName, fmt.Sprintf("C%d", rowIndex), post.UIDIP)
		f.SetCellValue(sheetName, fmt.Sprintf("D%d", rowIndex), post.isIP)
		f.SetCellValue(sheetName, fmt.Sprintf("E%d", rowIndex), post.PostNum)
		f.SetCellValue(sheetName, fmt.Sprintf("F%d", rowIndex), post.ComNum)
		f.SetCellValue(sheetName, fmt.Sprintf("G%d", rowIndex), totalActivity)
		rowIndex++
	}
	autoFilterRange := fmt.Sprintf("A1:G%d", rowIndex-1)
	f.AutoFilter(sheetName, autoFilterRange, nil)
	return f.SaveAs(filename)
}

func getR2Client() (*s3.Client, string, error) {
	accountId := os.Getenv("CF_ACCOUNT_ID")
	accessKeyId := os.Getenv("CF_ACCESS_KEY_ID")
	secretAccessKey := os.Getenv("CF_SECRET_ACCESS_KEY")
	bucketName := os.Getenv("CF_BUCKET_NAME")

	if accountId == "" || accessKeyId == "" || secretAccessKey == "" || bucketName == "" {
		return nil, "", fmt.Errorf("R2 인증 정보 누락")
	}

	r2Resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{ URL: fmt.Sprintf("https://%s.r2.cloudflarestorage.com", accountId) }, nil
	})

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithEndpointResolverWithOptions(r2Resolver),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKeyId, secretAccessKey, "")),
		config.WithRegion("auto"),
	)
	if err != nil { return nil, "", err }
	return s3.NewFromConfig(cfg), bucketName, nil
}

func uploadToR2(localFilename string, r2Key string) error {
	client, bucketName, err := getR2Client()
	if err != nil { return err }

	file, err := os.Open(localFilename)
	if err != nil { return err }
	defer file.Close()

	contentType := "application/octet-stream"
	if strings.HasSuffix(localFilename, ".xlsx") { contentType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" } else if strings.HasSuffix(localFilename, ".json") { contentType = "application/json" } else if strings.HasSuffix(localFilename, ".txt") { contentType = "text/plain" }

	_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{ Bucket: aws.String(bucketName), Key: aws.String(r2Key), Body: file, ContentType: aws.String(contentType) })
	return err
}

func downloadFromR2(objectKey string, localFilename string) error {
	client, bucketName, err := getR2Client()
	if err != nil { return err }
	file, err := os.Create(localFilename)
	if err != nil { return err }
	defer file.Close()
	result, err := client.GetObject(context.TODO(), &s3.GetObjectInput{ Bucket: aws.String(bucketName), Key: aws.String(objectKey) })
	if err != nil { return err } 
	defer result.Body.Close()
	_, err = io.Copy(file, result.Body)
	return err
}

func deleteFromR2(objectKey string) error {
	client, bucketName, err := getR2Client()
	if err != nil { return err }
	_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{ Bucket: aws.String(bucketName), Key: aws.String(objectKey) })
	return err
}

func getLastSavedTimeFromR2() (time.Time, error) {
	err := downloadFromR2("last_time.txt", "last_time.txt")
	if err != nil { return time.Time{}, fmt.Errorf("R2 기록 없음") }
	defer os.Remove("last_time.txt")
	data, err := os.ReadFile("last_time.txt")
	if err != nil { return time.Time{}, err }
	timeStr := strings.TrimSpace(string(data))
	parsedTime, err := time.ParseInLocation("2006-01-02 15:00", timeStr, kstLoc)
	if err != nil { return time.Time{}, err }
	sysLog("SYSTEM", "R2 동기화 완료. 마지막 수집 시간: %s", parsedTime.Format("2006-01-02 15:00"))
	return parsedTime, nil
}

func forceGC() {
	runtime.GC()
	debug.FreeOSMemory()
}

func main() {
	sysLog("SYSTEM", "============ 크롤러 가동 시작 ============")
	now := time.Now().In(kstLoc)
	limitTime := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), 0, 0, 0, kstLoc)

	lastTime, err := getLastSavedTimeFromR2()
	if err != nil || lastTime.IsZero() {
		sysLog("SYSTEM", "마지막 저장 시간 로드 실패 (%v). 2시간 전으로 강제 지정합니다.", err)
		lastTime = limitTime.Add(-2 * time.Hour)
	}

	// ⚡ [치명적 버그 수정 완료] 과거 시간대만 수집하도록 Before를 적용했습니다.
	for t := lastTime.Add(time.Hour); t.Before(limitTime); t = t.Add(time.Hour) {

		targetStart := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, kstLoc)
		targetEnd := targetStart.Add(time.Hour)
		scanStart := targetStart.Add(-1 * time.Hour)

		collectionTimeStr := targetStart.Format("2006-01-02 15:04")
		jsonFilename := fmt.Sprintf("%s_%02dh.json", targetStart.Format("2006-01-02"), targetStart.Hour())
		excelFilename := fmt.Sprintf("%s_%02dh.xlsx", targetStart.Format("2006-01-02"), targetStart.Hour())
		checkpointFilename := fmt.Sprintf("checkpoint_%s_%02dh.json", targetStart.Format("2006-01-02"), targetStart.Hour())

		var validPosts []int
		dataMap = make(map[string]*PostData)

		sysLog("SYSTEM", "▶️ 시간대 작업 시작: %s ~ %s", targetStart.Format("2006-01-02 15:00"), targetEnd.Format("15:00"))

		err = downloadFromR2(checkpointFilename, checkpointFilename)
		if err == nil {
			fileData, _ := os.ReadFile(checkpointFilename)
			var cp CheckpointData
			json.Unmarshal(fileData, &cp)
			validPosts = cp.RemainingPosts
			dataMap = cp.SavedData 
			os.Remove(checkpointFilename)
			sysLog("SYSTEM", "♻️ 체크포인트 복구 성공. 남은 %d개의 게시글 수집 재개", len(validPosts))
		} else {
			var findErr error
			validPosts, _, _, findErr = findTargetHourPosts(scanStart, targetEnd)
			if findErr != nil {
				sysLog("CRITICAL", "목록 탐색 중 에러 발생: %v -> 작업 즉시 중단", findErr)
				break
			}
			sysLog("SYSTEM", "🔍 시간대 내 총 발견된 게시글 수: %d개", len(validPosts))
		}

		if len(validPosts) > 0 {
			chunkSize := 500
			totalToProcess := len(validPosts)

			for len(validPosts) > 0 {
				end := chunkSize
				if end > len(validPosts) { end = len(validPosts) }
				chunk := validPosts[:end]
				
				err := scrapePostsAndComments(chunk, collectionTimeStr, targetStart, targetEnd)
				if err != nil {
					sysLog("CRITICAL", "수집 중 심각한 에러 발생: %v -> 다음 청크 진행을 포기하고 작업을 중단합니다.", err)
					break
				}

				validPosts = validPosts[end:]

				cp := CheckpointData{ RemainingPosts: validPosts, SavedData: dataMap }
				saveCheckpointLocal(checkpointFilename, &cp)
				uploadToR2(checkpointFilename, checkpointFilename)
				os.Remove(checkpointFilename)

				sysLog("SYSTEM", "💾 500개 단위 저장. 진행률: %d / %d", totalToProcess-len(validPosts), totalToProcess)
			}

			if len(validPosts) == 0 {
				if err := saveJsonLocal(jsonFilename); err == nil {
					uploadToR2(jsonFilename, strings.Replace(jsonFilename, "_", "/", 1))
					timeStr := targetStart.Format("2006-01-02 15:00")
					os.WriteFile("last_time.txt", []byte(timeStr), 0644)
					uploadToR2("last_time.txt", "last_time.txt")
					os.Remove("last_time.txt")
					os.Remove(jsonFilename)
				}
				if err := saveExcelLocal(excelFilename); err == nil {
					uploadToR2(excelFilename, strings.Replace(excelFilename, "_", "/", 1))
					os.Remove(excelFilename)
				}
				deleteFromR2(checkpointFilename)
				sysLog("SYSTEM", "✅ 시간대 %s 작업 완료 및 업로드 성공", targetStart.Format("15:00"))
			} else {
				sysLog("CRITICAL", "게시글이 남아있지만 중단됨. 남은 개수: %d", len(validPosts))
				break
			}
		} else {
			sysLog("SYSTEM", "⏭️ 수집할 게시글이 0개입니다. 스킵 처리합니다.")
			timeStr := targetStart.Format("2006-01-02 15:00")
			os.WriteFile("last_time.txt", []byte(timeStr), 0644)
			uploadToR2("last_time.txt", "last_time.txt")
			os.Remove("last_time.txt")
		}
		time.Sleep(3 * time.Second)
		forceGC()
	}
	sysLog("SYSTEM", "============ 크롤러 가동 종료 ============")
}
