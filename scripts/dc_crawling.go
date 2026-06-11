package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"math/rand"
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

// 💡 다양한 브라우저 정보 리스트 (User-Agent 스푸핑용)
var userAgents = []string{
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
}

// 💡 무작위로 User-Agent를 선택해주는 헬퍼 함수
func getRandomUA() string {
	return userAgents[rand.Intn(len(userAgents))]
}

func init() {
	var err error
	kstLoc, err = time.LoadLocation("Asia/Seoul")
	if err != nil {
		kstLoc = time.FixedZone("KST", 9*60*60)
	}
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

// 💡 메인 게시글 탐색 함수
func findTargetHourPosts(targetStart, targetEnd time.Time) ([]int, string, string, error) {
	c := colly.NewCollector(
		colly.UserAgent(getRandomUA()), // 💡 매번 무작위 브라우저 정보 사용 (차단 회피)
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
	var postsOnPage int          // 💡 현재 페이지의 게시글 수 추적 (섀도우 밴 감지용)
	var lastTitle string         // 💡 디버그용: 마지막으로 본 텍스트
	var lastParsed time.Time     // 💡 디버그용: 마지막으로 파싱된 시간

	c.OnError(func(r *colly.Response, err error) {
		if r.StatusCode == 403 || r.StatusCode == 503 {
			networkErr = fmt.Errorf("디시인사이드 서버 밴 (HTTP %d)", r.StatusCode)
			done = true 
		}
	})

	c.OnHTML("tr.ub-content", func(e *colly.HTMLElement) {
		if done { return }
		
		// 💡 정상적인 게시판 HTML이 내려왔다면 카운트 증가
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
				// 당일 글 (HH:MM 형식)
				todayStr := time.Now().In(kstLoc).Format("2006-01-02 ") + title + ":00"
				postTime, _ = time.ParseInLocation("2006-01-02 15:04:05", todayStr, kstLoc)
				
				// 💡 자정을 막 넘겼을 때, 어제 밤에 쓴 글을 오늘 밤(미래)으로 오해하는 버그 차단
				if postTime.After(time.Now().In(kstLoc)) {
					postTime = postTime.Add(-24 * time.Hour)
				}
			} else if strings.Count(title, ".") == 1 {
				// 올해 과거 글 (MM.DD 형식)
				thisYearStr := fmt.Sprintf("%d-%s 00:00:00", time.Now().In(kstLoc).Year(), strings.ReplaceAll(title, ".", "-"))
				postTime, _ = time.ParseInLocation("2006-01-02 15:04:05", thisYearStr, kstLoc)
			} else if strings.Count(title, ".") == 2 {
				// 작년 이전 글 (YY.MM.DD 형식)
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
		postsOnPage = 0 // 페이지를 방문할 때마다 카운트 초기화
		pageURL := fmt.Sprintf("https://gall.dcinside.com/mgallery/board/lists/?id=projectmx&page=%d", page)
		
		// 💡 기계적인 패턴(매크로 의심)을 피하기 위한 1.5초 ~ 3.5초 무작위 딜레이 추가
		sleepTime := time.Duration(1500+rand.Intn(2000)) * time.Millisecond
		time.Sleep(sleepTime) 
		
		c.Visit(pageURL)

		if networkErr != nil {
			return nil, "", "", networkErr
		}

		// 💡 빈 화면 섀도우 밴 감지
		if postsOnPage == 0 {
			return nil, "", "", fmt.Errorf("페이지 %d에서 게시글이 하나도 로딩되지 않았습니다. (디시인사이드 캡차 차단/섀도우 밴 확실시 됨)", page)
		}

		if page > 500 {
			// 디버그 상세 정보 출력
			return nil, "", "", fmt.Errorf("페이지 탐색 한계 초과. [디버그] 마지막 파싱 텍스트: '%s' -> 시간: %v (목표: %v)", lastTitle, lastParsed.Format("2006-01-02 15:04"), targetStart.Format("2006-01-02 15:04"))
		}
		page++
	}

	return validPostNumbers, startDate, endDate, nil
}




func scrapePostsAndComments(validPosts []int, collectionTimeStr string, targetStart, targetEnd time.Time) error {
	c := colly.NewCollector(
		colly.UserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
		colly.Async(true),
	)
	c.SetRequestTimeout(60 * time.Second)
	
	c.Limit(&colly.LimitRule{
		DomainGlob:  "*",
		Parallelism: 5,
		Delay:       1 * time.Second,
		RandomDelay: 500 * time.Millisecond,
	})

	var visitedPosts sync.Map
	var failCount int32
	var wg sync.WaitGroup

	c.OnError(func(r *colly.Response, err error) {
		if r.StatusCode == 404 { return }
		retries, _ := strconv.Atoi(r.Ctx.Get("retry_count"))
		if r.StatusCode >= 500 || r.StatusCode == 0 {
			if retries < 3 {
				r.Ctx.Put("retry_count", strconv.Itoa(retries+1))
				time.Sleep(1 * time.Second)
				r.Request.Retry()
			} else {
				atomic.AddInt32(&failCount, 1)
			}
		}
	})

	c.OnRequest(func(r *colly.Request) {
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
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
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
			data := url.Values{}
			data.Set("id", "projectmx")
			data.Set("no", sno)
			data.Set("cmt_id", "projectmx")
			data.Set("cmt_no", sno)
			data.Set("e_s_n_o", esno)
			data.Set("comment_page", strconv.Itoa(page))
			data.Set("_GALLTYPE_", "M")

			req, _ := http.NewRequest("POST", endpoint, strings.NewReader(data.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.Header.Set("Referer", headerurl)
			req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
			req.Header.Set("X-Requested-With", "XMLHttpRequest")

			resp, err := sharedClient.Do(req)
			if err == nil {
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
	}

	_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:      aws.String(bucketName),
		Key:         aws.String(r2Key),
		Body:        file,
		ContentType: aws.String(contentType),
	})

	return err
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

func getLastSavedTime() (time.Time, error) {
	client, bucketName, err := getR2Client()
	if err != nil { return time.Time{}, err }

	var maxTime time.Time
	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.TODO())
		if err != nil { return time.Time{}, err }
		
		for _, obj := range page.Contents {
			key := *obj.Key
			if !strings.HasSuffix(key, ".xlsx") { continue }
			normalizedKey := strings.Replace(key, "/", "_", 1)
			datePart := strings.TrimSuffix(normalizedKey, ".xlsx")
			parsedTime, err := time.ParseInLocation("2006-01-02_15h", datePart, kstLoc)
			if err != nil { continue }
			
			if parsedTime.After(maxTime) { maxTime = parsedTime }
		}
	}
	return maxTime, nil
}

func forceGC() {
	runtime.GC()
	debug.FreeOSMemory()
}

func main() {
	now := time.Now().In(kstLoc)
	limitTime := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), 0, 0, 0, kstLoc)
	
	lastTime, _ := getLastSavedTime()

	if lastTime.IsZero() || time.Since(lastTime) > 24*time.Hour {
		lastTime = limitTime.Add(-2 * time.Hour) 
	}

	for t := lastTime.Add(time.Hour); t.Before(limitTime.Add(time.Hour)); t = t.Add(time.Hour) {
		
		if t.After(limitTime) || t.Equal(limitTime) {
			break
		}

		targetStart := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, kstLoc)
		targetEnd := targetStart.Add(time.Hour)
		scanStart := targetStart.Add(-1 * time.Hour)

		collectionTimeStr := targetStart.Format("2006-01-02 15:04")
		jsonFilename := fmt.Sprintf("%s_%02dh.json", targetStart.Format("2006-01-02"), targetStart.Hour())
		excelFilename := fmt.Sprintf("%s_%02dh.xlsx", targetStart.Format("2006-01-02"), targetStart.Hour())

		dataMap = make(map[string]*PostData)

		// 💡 시작 시각 및 대상 시간대 출력
		fmt.Printf("[%s] ▶️ 작업 시작 (대상 시간대: %s ~ %s)\n", time.Now().In(kstLoc).Format("15:04:05"), targetStart.Format("2006-01-02 15:00"), targetEnd.Format("15:00"))

		validPosts, _, _, err := findTargetHourPosts(scanStart, targetEnd)
		if err != nil {
			fmt.Printf("[%s] ❌ [ERROR] 게시글 목록 탐색 중 오류 발생: %v\n", time.Now().In(kstLoc).Format("15:04:05"), err)
			continue
		}

		// 💡 찾은 게시글 수 출력
		fmt.Printf("[%s] 🔍 발견된 게시글 수: %d개\n", time.Now().In(kstLoc).Format("15:04:05"), len(validPosts))

		if len(validPosts) > 0 {
			err := scrapePostsAndComments(validPosts, collectionTimeStr, targetStart, targetEnd)
			if err != nil {
				// 💡 상세 내용 및 댓글 수집 중 크리티컬 오류 출력 (데이터 누수 의심 로그)
				fmt.Printf("[%s] ❌ [ERROR] 수집 중 과도한 오류 발생 (데이터 누수 의심): %v\n", time.Now().In(kstLoc).Format("15:04:05"), err)
				continue
			}

			if err := saveJsonLocal(jsonFilename); err == nil {
				r2JsonKey := strings.Replace(jsonFilename, "_", "/", 1)
				uploadToR2(jsonFilename, r2JsonKey)
				uploadToR2(jsonFilename, "latest_data.json")
				os.Remove(jsonFilename)
			}

			if err := saveExcelLocal(excelFilename); err == nil {
				r2ExcelKey := strings.Replace(excelFilename, "_", "/", 1)
				uploadToR2(excelFilename, r2ExcelKey)
				os.Remove(excelFilename)
			}
			
			// 💡 완료 시각 및 대상 시간대 출력
			fmt.Printf("[%s] ✅ 작업 완료 및 업로드 성공\n", time.Now().In(kstLoc).Format("15:04:05"))
		} else {
			fmt.Printf("[%s] ⏭️ 수집할 게시글이 없어 건너뜁니다.\n", time.Now().In(kstLoc).Format("15:04:05"))
		}
		
		// 💡 가독성을 위한 구분선 추가
		fmt.Println(strings.Repeat("-", 50))
		time.Sleep(3 * time.Second)
		forceGC()
	}
}
