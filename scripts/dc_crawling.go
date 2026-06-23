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

// 💡 체크포인트 데이터 저장을 위한 구조체
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
	banUntil time.Time // 밴이 풀리는 시간 기록
)

// 💡 1. 밴 상태 체크: 밴 기간이면 풀릴 때까지 모든 스레드가 여기서 대기합니다.
func checkBanState() {
	banMutex.Lock()
	if time.Now().Before(banUntil) {
		sleepDur := time.Until(banUntil)
		banMutex.Unlock()
		time.Sleep(sleepDur) // 밴이 풀릴 때까지 스레드 정지
		return
	}
	banMutex.Unlock()
}

// 💡 2. 밴 발동: 403 감지 시 기준 시간을 현재부터 1분 뒤로 설정합니다.
func triggerBan() {
	banMutex.Lock()
	defer banMutex.Unlock()
	// 여러 스레드가 동시에 에러를 내더라도, 대기 시간은 최초 1회만 1분으로 늘어납니다.
	if time.Now().After(banUntil) {
		banUntil = time.Now().Add(2 * time.Minute)
	}
}


func getRandomUA() string {
	return userAgents[rand.Intn(len(userAgents))]
}

// 💡 중간 저장본을 로컬에 저장하는 함수
func saveCheckpointLocal(filename string, cp *CheckpointData) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(cp)
}

// 💡 처리가 완료된 후 R2에서 체크포인트 파일을 청소하는 함수
func deleteFromR2(objectKey string) error {
	client, bucketName, err := getR2Client()
	if err != nil {
		return err
	}
	_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	return err
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
	c := colly.NewCollector(
		colly.UserAgent(getRandomUA()),
	)
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
	var lastTitle string         
	var lastParsed time.Time     

	c.OnError(func(r *colly.Response, err error) {
		if r.StatusCode == 403 || r.StatusCode == 503 {
			networkErr = fmt.Errorf("디시인사이드 서버 밴 (HTTP %d)", r.StatusCode)
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
		if title == "" {
			title = strings.TrimSpace(e.ChildText("td.gall_date"))
		}

		lastTitle = title 

		var postTime time.Time
		var parseErr error

		postTime, parseErr = time.ParseInLocation("2006-01-02 15:04:05", title, kstLoc)

		if parseErr != nil {
			if strings.Contains(title, ":") && !strings.Contains(title, "-") && !strings.Contains(title, ".") {
				todayStr := time.Now().In(kstLoc).Format("2006-01-02 ") + title + ":00"
				postTime, _ = time.ParseInLocation("2006-01-02 15:04:05", todayStr, kstLoc)
				
				if postTime.After(time.Now().In(kstLoc)) {
					postTime = postTime.Add(-24 * time.Hour)
				}
			} else if strings.Count(title, ".") == 1 {
				thisYearStr := fmt.Sprintf("%d-%s 00:00:00", time.Now().In(kstLoc).Year(), strings.ReplaceAll(title, ".", "-"))
				postTime, _ = time.ParseInLocation("2006-01-02 15:04:05", thisYearStr, kstLoc)
			} else if strings.Count(title, ".") == 2 {
				parts := strings.Split(title, ".")
				if len(parts[0]) == 2 {
					title = "20" + title
				}
				pastStr := strings.ReplaceAll(title, ".", "-") + " 00:00:00"
				postTime, _ = time.ParseInLocation("2006-01-02 15:04:05", pastStr, kstLoc)
			}
		}

		if postTime.IsZero() {
			postTime = time.Date(2000, 1, 1, 0, 0, 0, 0, kstLoc)
		}

		lastParsed = postTime

		if (postTime.Equal(targetStart) || postTime.After(targetStart)) && postTime.Before(targetEnd) {
			consecutiveOldPosts = 0
			validPostNumbers = append(validPostNumbers, postNo)
			
			if endDate == "" { endDate = title }
			startDate = title
		}

		if postTime.Before(targetStart) {
			consecutiveOldPosts++
			if consecutiveOldPosts >= maxConsecutiveOld { done = true }
		} else {
			if postTime.After(targetEnd) || postTime.Equal(targetEnd) { consecutiveOldPosts = 0 }
		}
	})

	for !done {
		postsOnPage = 0 
		pageURL := fmt.Sprintf("https://gall.dcinside.com/mgallery/board/lists/?id=projectmx&page=%d", page)
		
		sleepTime := time.Duration(1500+rand.Intn(2000)) * time.Millisecond
		time.Sleep(sleepTime) 
		
		c.Visit(pageURL)

		if networkErr != nil {
			return nil, "", "", networkErr
		}

		if postsOnPage == 0 {
			return nil, "", "", fmt.Errorf("페이지 %d에서 게시글이 하나도 로딩되지 않았습니다. (디시인사이드 캡차 차단/섀도우 밴 확실시 됨)", page)
		}

		if page > 5000 {
			return nil, "", "", fmt.Errorf("페이지 탐색 한계 초과. [디버그] 마지막 파싱 텍스트: '%s' -> 시간: %v (목표: %v)", lastTitle, lastParsed.Format("2006-01-02 15:04"), targetStart.Format("2006-01-02 15:04"))
		}
		page++
	}

	return validPostNumbers, startDate, endDate, nil
}

func scrapePostsAndComments(validPosts []int, collectionTimeStr string, targetStart, targetEnd time.Time) error {
	c := colly.NewCollector(
		colly.UserAgent(getRandomUA()),
		colly.Async(true),
	)
	c.SetRequestTimeout(60 * time.Second)
	
	c.Limit(&colly.LimitRule{
		DomainGlob:  "*",
		Parallelism: 3,               
		Delay:       10 * time.Second, 
		RandomDelay: 2 * time.Second, 
	})

	var visitedPosts sync.Map
	var failCount int32
	var wg sync.WaitGroup

	c.OnError(func(r *colly.Response, err error) {
		if r.StatusCode == 404 { return } 

		retries, _ := strconv.Atoi(r.Ctx.Get("retry_count"))

		// ⚡ 403 밴 발생 시
		if r.StatusCode == 403 || r.StatusCode == 503 {
			triggerBan() // 1. 즉시 전역 1분 정지 발동
			
			if retries < 3 { 
				r.Ctx.Put("retry_count", strconv.Itoa(retries+1))
				r.Request.Retry() // 2. 실패한 게시글을 다시 큐에 집어넣음 (최우선 재시도)
				return
			} else {
				atomic.AddInt32(&failCount, 1)
				return
			}
		}

		// 기타 서버 에러 처리
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

	// ⚡ 모든 요청을 보내기 직전에 '정지 신호등'이 켜져 있는지 확인
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

		wg.Add(1)
		go func(postNo int, postEsno string) {
			defer wg.Done()
			commentSrc(postNo, postEsno, collectionTimeStr, targetStart, targetEnd)
		}(no, esno)
	})

	for _, no := range validPosts {
		url := fmt.Sprintf("https://gall.dcinside.com/mgallery/board/view/?id=projectmx&no=%d", no)
		c.Visit(url)
	}
	
	c.Wait() 
	wg.Wait() 

	finalFailCount := atomic.LoadInt32(&failCount)
	if finalFailCount > 15 {
		return fmt.Errorf("수집 실패 과다 (데이터 누수 가능성 있음)")
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
	if esno == "" { return }

	endpoint := "https://gall.dcinside.com/board/comment/"
	sno := strconv.Itoa(no)
	headerurl := fmt.Sprintf("https://gall.dcinside.com/mgallery/board/view/?id=projectmx&no=%d&t=cv", no)

	page := 1
	for {
		var body []byte
		var reqErr error

		for retry := 0; retry < 3; retry++ {
			checkBanState() // ⚡ 댓글 수집 전에도 밴 상태인지 확인하고 대기

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
				// ⚡ 댓글 서버에서 403 차단이 떨어지면 전역 1분 정지 발동
				if resp.StatusCode == 403 || resp.StatusCode == 503 {
					triggerBan() 
					resp.Body.Close()
					continue // 다음 retry 루프로 넘어가 1분 대기 후 재시도
				}

				body, reqErr = io.ReadAll(resp.Body)
				resp.Body.Close()
				if reqErr == nil && len(body) > 0 { break }
			}
			time.Sleep(500 * time.Millisecond)
		}

		if len(body) == 0 { break }

		var responseData ResponseData
		if err := json.Unmarshal(body, &responseData); err != nil { break }
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
			if strings.Count(reg, ":") == 1 {
				reg += ":00" 
			}

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
		time.Sleep(100 * time.Millisecond)
	}
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
	if err := f.AutoFilter(sheetName, autoFilterRange, nil); err != nil { return err }
	if err := f.SaveAs(filename); err != nil { return err }
	return nil
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
		return aws.Endpoint{
			URL: fmt.Sprintf("https://%s.r2.cloudflarestorage.com", accountId),
		}, nil
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
	if strings.HasSuffix(localFilename, ".xlsx") {
		contentType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
	} else if strings.HasSuffix(localFilename, ".json") {
		contentType = "application/json"
	} else if strings.HasSuffix(localFilename, ".txt") {
		contentType = "text/plain"
	}

	_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:      aws.String(bucketName),
		Key:         aws.String(r2Key),
		Body:        file,
		ContentType: aws.String(contentType),
	})

	return err
}

func downloadFromR2(objectKey string, localFilename string) error {
	client, bucketName, err := getR2Client()
	if err != nil { return err }

	file, err := os.Create(localFilename)
	if err != nil { return err }
	defer file.Close()

	result, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil { return err } 
	defer result.Body.Close()

	_, err = io.Copy(file, result.Body)
	return err
}

func getLastSavedTimeFromR2() (time.Time, error) {
	err := downloadFromR2("last_time.txt", "last_time.txt")
	if err != nil {
		return time.Time{}, fmt.Errorf("R2에 저장된 시간 기록이 없습니다: %v", err)
	}
	defer os.Remove("last_time.txt")

	data, err := os.ReadFile("last_time.txt")
	if err != nil {
		return time.Time{}, err
	}

	timeStr := strings.TrimSpace(string(data))
	parsedTime, err := time.ParseInLocation("2006-01-02 15:00", timeStr, kstLoc)
	if err != nil {
		return time.Time{}, fmt.Errorf("시간 문자열 파싱 실패: %v", err)
	}

	fmt.Printf("📂 [R2 동기화] 마지막으로 수집 완료된 시간: %s\n", parsedTime.Format("2006-01-02 15:00"))
	return parsedTime, nil
}

func forceGC() {
	runtime.GC()
	debug.FreeOSMemory()
}

func main() {
	now := time.Now().In(kstLoc)
	limitTime := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), 0, 0, 0, kstLoc)

	lastTime, err := getLastSavedTimeFromR2()

	if err != nil || lastTime.IsZero() {
		fmt.Printf("⚠️ %v\n기본값(현재 시간 기준 2시간 전)으로 수집을 시작합니다.\n", err)
		lastTime = limitTime.Add(-2 * time.Hour)
	}

	for t := lastTime.Add(time.Hour); !t.After(limitTime); t = t.Add(time.Hour) {

		targetStart := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, kstLoc)
		targetEnd := targetStart.Add(time.Hour)
		scanStart := targetStart.Add(-1 * time.Hour)

		collectionTimeStr := targetStart.Format("2006-01-02 15:04")
		jsonFilename := fmt.Sprintf("%s_%02dh.json", targetStart.Format("2006-01-02"), targetStart.Hour())
		excelFilename := fmt.Sprintf("%s_%02dh.xlsx", targetStart.Format("2006-01-02"), targetStart.Hour())
		checkpointFilename := fmt.Sprintf("checkpoint_%s_%02dh.json", targetStart.Format("2006-01-02"), targetStart.Hour())

		var validPosts []int
		dataMap = make(map[string]*PostData)

		fmt.Printf("[%s] ▶️ 작업 시작 (대상 시간대: %s ~ %s)\n", time.Now().In(kstLoc).Format("15:04:05"), targetStart.Format("2006-01-02 15:00"), targetEnd.Format("15:00"))

		// ⚡ [1단계] R2에 체크포인트(중간 저장본)가 있는지 확인
		err = downloadFromR2(checkpointFilename, checkpointFilename)
		if err == nil {
			// 체크포인트가 존재하면 로드 (탐색 스킵)
			fileData, _ := os.ReadFile(checkpointFilename)
			var cp CheckpointData
			json.Unmarshal(fileData, &cp)
			
			validPosts = cp.RemainingPosts
			dataMap = cp.SavedData // 이전에 작업했던 유저 데이터 복구
			os.Remove(checkpointFilename)

			fmt.Printf("♻️ [복구 성공] 6시간 강제 종료로 멈췄던 체크포인트를 불러왔습니다.\n")
			fmt.Printf("⏭️ 게시글 탐색을 스킵하고, 남은 %d개의 게시글부터 수집을 재개합니다.\n", len(validPosts))
		} else {
			// 체크포인트가 없으면 정상적으로 게시글 탐색 시작
			var findErr error
			validPosts, _, _, findErr = findTargetHourPosts(scanStart, targetEnd)
			if findErr != nil {
				fmt.Printf("[%s] ❌ [ERROR] 게시글 목록 탐색 중 오류 발생: %v\n", time.Now().In(kstLoc).Format("15:04:05"), findErr)
				fmt.Println("🚨 데이터 누락을 방지하기 위해 전체 작업을 즉시 중단합니다.")
				break
			}
			fmt.Printf("[%s] 🔍 발견된 게시글 수: %d개\n", time.Now().In(kstLoc).Format("15:04:05"), len(validPosts))
		}

		if len(validPosts) > 0 {
			// ⚡ [2단계] 한 번에 다 하지 않고 500개씩 쪼개서(Chunk) 작업
			chunkSize := 500
			totalToProcess := len(validPosts)

			for len(validPosts) > 0 {
				end := chunkSize
				if end > len(validPosts) {
					end = len(validPosts)
				}
				
				chunk := validPosts[:end]
				
				// 500개만 수집
				err := scrapePostsAndComments(chunk, collectionTimeStr, targetStart, targetEnd)
				if err != nil {
					fmt.Printf("[%s] ❌ [ERROR] 수집 중 오류 발생: %v\n", time.Now().In(kstLoc).Format("15:04:05"), err)
					fmt.Println("🚨 작업을 중단합니다. 다음 스케줄에서 남은 분량을 재시도합니다.")
					break
				}

				// 성공적으로 수집한 500개를 목록에서 제거
				validPosts = validPosts[end:]

				// ⚡ [3단계] 중간 세이브 데이터 R2에 업로드
				cp := CheckpointData{
					RemainingPosts: validPosts,
					SavedData:      dataMap,
				}
				saveCheckpointLocal(checkpointFilename, &cp)
				uploadToR2(checkpointFilename, checkpointFilename)
				os.Remove(checkpointFilename)

				processedSoFar := totalToProcess - len(validPosts)
				fmt.Printf("💾 [Save Point] 500개 단위 중간 저장 완료. (진행률: %d / %d)\n", processedSoFar, totalToProcess)
			}

			// 위 루프를 무사히 빠져나왔다면 (남은 게시글이 0개라면) 최종 파일 생성
			if len(validPosts) == 0 {
				if err := saveJsonLocal(jsonFilename); err == nil {
					r2JsonKey := strings.Replace(jsonFilename, "_", "/", 1)
					uploadToR2(jsonFilename, r2JsonKey)

					timeStr := targetStart.Format("2006-01-02 15:00")
					os.WriteFile("last_time.txt", []byte(timeStr), 0644)
					uploadToR2("last_time.txt", "last_time.txt")
					os.Remove("last_time.txt")
					os.Remove(jsonFilename)
				}

				if err := saveExcelLocal(excelFilename); err == nil {
					r2ExcelKey := strings.Replace(excelFilename, "_", "/", 1)
					uploadToR2(excelFilename, r2ExcelKey)
					os.Remove(excelFilename)
				}

				// ⚡ [4단계] 완벽하게 끝났으니 R2에 남아있는 중간 세이브 파일(체크포인트) 삭제
				deleteFromR2(checkpointFilename)
				fmt.Printf("[%s] ✅ 작업 완료 및 업로드 성공\n", time.Now().In(kstLoc).Format("15:04:05"))
			} else {
				// 에러로 루프를 빠져나온 경우 다음 스케줄러로 넘기기 위해 종료
				break
			}
		} else {
			fmt.Printf("[%s] ⏭️ 수집할 게시글이 없어 건너뜁니다.\n", time.Now().In(kstLoc).Format("15:04:05"))
			
			timeStr := targetStart.Format("2006-01-02 15:00")
			os.WriteFile("last_time.txt", []byte(timeStr), 0644)
			uploadToR2("last_time.txt", "last_time.txt")
			os.Remove("last_time.txt")
		}

		fmt.Println(strings.Repeat("-", 50))
		time.Sleep(3 * time.Second)
		forceGC()
	}
}
