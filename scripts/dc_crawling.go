package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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

// 웹 대시보드(JSON)용 구조체
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
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     90 * time.Second,
		},
	}
	globalEsno      string
	globalEsnoMutex sync.RWMutex
	esnoRegex       = regexp.MustCompile(`<input[^>]+id="e_s_n_o"[^>]+value="([^"]+)"`)
)

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

func findTargetHourPosts(targetStart, targetEnd time.Time) ([]int, string, string, error) {
	c := colly.NewCollector(
		colly.UserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
	)
	c.SetRequestTimeout(30 * time.Second)

	var validPostNumbers []int
	var startDate, endDate string

	page := 1
	done := false
	visitedIDs := make(map[int]bool)
	consecutiveOldPosts := 0
	const maxConsecutiveOld = 15

	c.OnHTML("tr.ub-content", func(e *colly.HTMLElement) {
		if done { return }
		if _, err := strconv.Atoi(e.ChildText("td.gall_num")); err != nil { return }

		subject := strings.TrimSpace(e.ChildText("td.gall_subject"))
		if subject == "설문" || subject == "AD" || subject == "공지" { return }

		noStr := e.Attr("data-no")
		postNo, err := strconv.Atoi(noStr)
		if err != nil { return }

		if visitedIDs[postNo] { return }
		visitedIDs[postNo] = true

		title := e.ChildAttr("td.gall_date", "title")
		if title == "" { title = e.ChildText("td.gall_date") }

		postTime, err := time.ParseInLocation("2006-01-02 15:04:05", title, kstLoc)
		if err != nil { return }

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
		pageURL := fmt.Sprintf("https://gall.dcinside.com/mgallery/board/lists/?id=projectmx&page=%d", page)
		time.Sleep(200 * time.Millisecond)
		c.Visit(pageURL)

		if page > 500 {
			return nil, "", "", fmt.Errorf("페이지 탐색 한계 초과")
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
		Parallelism: 2,
		Delay:       3 * time.Second,
		RandomDelay: 2 * time.Second,
	})

	var visitedPosts sync.Map
	var failCount int32

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
		commentSrc(no, esno, collectionTimeStr, targetStart, targetEnd)
	})

	for _, no := range validPosts {
		url := fmt.Sprintf("https://gall.dcinside.com/mgallery/board/view/?id=projectmx&no=%d", no)
		c.Visit(url)
	}
	c.Wait()

	finalFailCount := atomic.LoadInt32(&failCount)
	if finalFailCount > 15 {
		return fmt.Errorf("수집 실패 과다")
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

			if strings.Count(reg, ".") == 2 {
				cTime, parseErr = time.ParseInLocation("2006.01.02 15:04:05", reg, kstLoc)
			} else if strings.Count(reg, ":") == 1 {
				fullDateStr := fmt.Sprintf("%d.%s", targetStart.Year(), reg)
				cTime, parseErr = time.ParseInLocation("2006.01.02 15:04", fullDateStr, kstLoc)
			} else {
				fullDateStr := fmt.Sprintf("%d.%s", targetStart.Year(), reg)
				cTime, parseErr = time.ParseInLocation("2006.01.02 15:04:05", fullDateStr, kstLoc)
			}

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

// 웹 대시보드를 위한 JSON 파일 생성 함수
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

// 확장자에 맞춰 동적으로 ContentType을 변경하여 R2에 업로드하는 함수
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
	lastTime, err := getLastSavedTime()

	if err != nil || lastTime.IsZero() || time.Since(lastTime) > 24*time.Hour {
		lastTime = limitTime.Add(-1 * time.Hour) 
	}

	for t := lastTime.Add(time.Hour); t.Before(limitTime); t = t.Add(time.Hour) {
		targetStart := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, kstLoc)
		targetEnd := targetStart.Add(time.Hour)
		scanStart := targetStart.Add(-2 * time.Hour)

		collectionTimeStr := targetStart.Format("2006-01-02 15:04")
		jsonFilename := fmt.Sprintf("%s_%02dh.json", targetStart.Format("2006-01-02"), targetStart.Hour())
		excelFilename := fmt.Sprintf("%s_%02dh.xlsx", targetStart.Format("2006-01-02"), targetStart.Hour())

		dataMap = make(map[string]*PostData)

		validPosts, _, _, err := findTargetHourPosts(scanStart, targetEnd)

		if err == nil && len(validPosts) > 0 {
			err := scrapePostsAndComments(validPosts, collectionTimeStr, targetStart, targetEnd)

			if err == nil {
				// 1. JSON 대시보드 데이터 생성 및 R2 업로드
				if err := saveJsonLocal(jsonFilename); err == nil {
					r2JsonKey := strings.Replace(jsonFilename, "_", "/", 1)
					uploadToR2(jsonFilename, r2JsonKey)
					
					// 웹페이지 최초 접속 시 바로 보여줄 수 있게 최신 데이터 덮어쓰기
					uploadToR2(jsonFilename, "latest_data.json")
					os.Remove(jsonFilename)
				}

				// 2. 엑셀 백업 데이터 생성 및 R2 업로드
				if err := saveExcelLocal(excelFilename); err == nil {
					r2ExcelKey := strings.Replace(excelFilename, "_", "/", 1)
					uploadToR2(excelFilename, r2ExcelKey)
					os.Remove(excelFilename)
				}
			}
		}
		time.Sleep(3 * time.Second)
		forceGC()
	}
}
