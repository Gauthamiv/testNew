package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sbgoclient"
	"strconv"
	"strings"
	"time"

	logInfo "github.com/Sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
)

var errLog *log.Logger
var reqlog *log.Logger
var today_date = time.Now().Local()
var static_name = "/static" + today_date.Format("2006-01-02-15-04-05")
var sbConfig sbgoclient.SBConnectorConfig

type RepoDetails struct {
	Id               int         `json:"id,omitempty"`
	Name             string      `json:"name,omitempty"`
	FullName         string      `json:"full_name,omitempty"`
	Owner            OwnerDetail `json:"owner,omitempty"`
	Private          bool        `json:"private,omitempty"`
	HtmlUrl          string      `json:"html_url,omitempty"`
	Description      string      `json:"description,omitempty"`
	Fork             bool        `json:"fork,omitempty"`
	ForksUrl         string      `json:"forks_url,omitempty"`
	Url              string      `json:"url,omitempty"`
	KeysUrl          string      `json:"keys_url,omitempty"`
	CollaboratorsUrl string      `json:"collaborators_url,omitempty"`
	TeamsUrl         string      `json:"teams_url,omitempty"`
	HooksUrl         string      `json:"hooks_url,omitempty"`
	IssueEventsUrl   string      `json:"issue_events_url,omitempty"`
	EventsUrl        string      `json:"events_url,omitempty"`
	AssigneesUrl     string      `json:"assignees_url,omitempty"`
	BranchesUrl      string      `json:"branches_url,omitempty"`
	TagsUrl          string      `json:"tags_url,omitempty"`
	BlobsUrl         string      `json:"blobs_url,omitempty"`
	GitTagsUrl       string      `json:"git_tags_url,omitempty"`
	GitRefsUrl       string      `json:"git_refs_url,omitempty"`
	TreesUrl         string      `json:"trees_url,omitempty"`
	StatusesUrl      string      `json:"statuses_url,omitempty"`
	LanguagesUrl     string      `json:"languages_url,omitempty"`
	StargazersUrl    string      `json:"stargazers_url,omitempty"`
	ContributorsUrl  string      `json:"contributors_url,omitempty"`
	SubscribersUrl   string      `json:"subscribers_url,omitempty"`
	SubscriptionUrl  string      `json:"subscription_url,omitempty"`
	CommitsUrl       string      `json:"commits_url,omitempty"`
	GitCommitsUrl    string      `json:"git_commits_url,omitempty"`
	CommentsUrl      string      `json:"comments_url,omitempty"`
	IssueCommentUrl  string      `json:"issue_comment_url,omitempty"`
	ContentsUrl      string      `json:"contents_url,omitempty"`
	CompareUrl       string      `json:"compare_url,omitempty"`
	MergesUrl        string      `json:"merges_url,omitempty"`
	ArchiveUrl       string      `json:"archive_url,omitempty"`
	DownloadsUrl     string      `json:"downloads_url,omitempty"`
	IssuesUrl        string      `json:"issues_url,omitempty"`
	PullsUrl         string      `json:"pulls_url,omitempty"`
	MilestonesUrl    string      `json:"milestones_url,omitempty"`
	NotificationsUrl string      `json:"notfications_url,omitempty"`
	LabelsUrl        string      `json:"labels_url,omitempty"`
	ReleasesUrl      string      `json:"releases_url,omitempty"`
	DeploymentsUrl   string      `json:"deployments_url,omitempty"`
}

type OwnerDetail struct {
	Id                int    `json:"id,omitempty"`
	Login             string `json:"login,omitempty"`
	AvatarUrl         string `json:"avatar_url,omitempty"`
	GravatarId        string `json:"gravatar_id,omitempty"`
	Url               string `json:"url,omitempty"`
	HtmlUrl           string `json:"html_url,omitempty"`
	FollowersUrl      string `json:"followers_url,omitempty"`
	FollowingUrl      string `json:"following_url,omitempty"`
	GistsUrl          string `json:"gists_url,omitempty"`
	StarredUrl        string `json:"starred_url,omitempty"`
	SubscriptionsUrl  string `json:"subscriptions_url,omitempty"`
	OrganizationsUrl  string `json:"organizations_url,omitempty"`
	ReposUrl          string `json:"repos_url,omitempty"`
	EventsUrl         string `json:"events_url,omitempty"`
	ReceivedEventsUrl string `json:"received_events_url,omitempty"`
	Type              string `json:"type,omitempty"`
	SiteAdmin         bool   `json:"site_admin,omitempty"`
}

type Contents struct {
	Name        string `json:"name,omitempty"`
	Path        string `json:"path,omitempty"`
	Sha         string `json:"sha,omitempty"`
	Size        int    `json:"size,omitempty"`
	Url         string `json:"url,omitempty"`
	HtmlUrl     string `json:"html_url,omitempty"`
	GitUrl      string `json:"git_url,omitempty"`
	DownloadUrl string `json:"download_url,omitempty"`
	Type        string `json:"type,omitempty"`
	Links       Link   `json:"_links,omitempty"`
}

var countErr int

type Link struct {
	Self string `json:"self,omitempty"`
	Git  string `json:"git,omitempty"`
	Html string `json:"html,omitempty"`
}

type Refresh_content struct {
	Uid          string `json:"uid,omitempty"`
	Url          string `json:"url,omitempty"`
	Lastmodified string `json:"lastmodified,omitempty"`
	Title        string `json:"title,omitempty"`
}
type Refresh_Result struct {
	Result []Refresh_content `json:"result,omitempty"`
	Hits   string            `json:"@hits,omitempty"`
}
type Refresh_Results struct {
	Results Refresh_Result `json:"results,omitempty"`
}
type Refresh_single_Result struct {
	Result Refresh_content `json:"result,omitempty"`
	Hits   string          `json:"@hits,omitempty"`
}
type Refresh_single_Results struct {
	Results Refresh_single_Result `json:"results,omitempty"`
}
type Refresh_Result_single struct {
	Result Refresh_content `json:"result,omitempty"`
	Hits   string          `json:"@hits,omitempty"`
}
type Refresh_Results_single struct {
	Results Refresh_Result_single `json:"results,omitempty"`
}

func init() {
	sbConfig = sbgoclient.LoadConfig("githubconnector.yml")
	e, err := os.OpenFile("error.log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)

	if err != nil {
		fmt.Printf("error opening file: %v", err)
		os.Exit(1)
	}
	errLog = log.New(e, "", log.Ldate|log.Ltime)
	errLog.SetOutput(&lumberjack.Logger{
		Filename:   "error.log",
		MaxSize:    sbConfig.Log_File_maxsize,    // megabytes after which new file is created
		MaxBackups: sbConfig.Log_File_maxbackups, // number of backups
		MaxAge:     sbConfig.Log_File_maxage,     //days
	})
	reql, err := os.OpenFile("custom.log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)

	if err != nil {
		fmt.Printf("error opening file: %v", err)
		os.Exit(1)
	}
	reqlog = log.New(reql, "", log.Ldate|log.Ltime)
	reqlog.SetOutput(&lumberjack.Logger{
		Filename:   "custom.log",
		MaxSize:    sbConfig.Log_File_maxsize,    // megabytes after which new file is created
		MaxBackups: sbConfig.Log_File_maxbackups, // number of backups
		MaxAge:     sbConfig.Log_File_maxage,     //days
	})
}

func main() {

	fmt.Println("Internal Server running...")
	today_date := time.Now().Local().UTC() //.Add(-10 * time.Minute)

	//sbConfig = sbgoclient.LoadConfig("githubconnector.yml")
	//reqlog.Println("Load config complete")
	start := time.Now()
	var refresh_content *Refresh_Results
	var refresh_content_single *Refresh_Results_single
	last_run_copy := today_date.Format("2006-01-02 15:04:05")
	last_run_copy_date := strings.Split(last_run_copy, " ")
	path_directory := sbConfig.DataDir + "/" + static_name
	err := os.Mkdir(path_directory, os.FileMode(0777))
	if err != nil {
		errLog.Println("error while making directory @line 176 is:", err)
	}

retry:
	checkUrl := strings.Split(sbConfig.URL, "rest")
	_, err = http.Get(checkUrl[0])
	if err != nil {
		if countErr > 2 {
			fmt.Println("the server is not connecting")
			return // nil, err
		} else {
			countErr++
			time.Sleep(time.Second * 10)
			goto retry
		}
	}
	countErr = 0

	publicRepo := sbConfig.PublicRepos
	if publicRepo {
		fmt.Println("public repos")
		err := getfilecontents(sbConfig.Githuburl+"/repositories", path_directory)
		if err != nil {
			return
		}
	}

	for i := 0; i < len(sbConfig.IncludeUsers); i++ {
		err := getfilecontents(sbConfig.Githuburl+"/users/"+sbConfig.IncludeUsers[i]+"/repos", path_directory)
		if err != nil {
			return
		}
	}
	for i := 0; i < len(sbConfig.IncludeOrgs); i++ {
		err := getfilecontents(sbConfig.Githuburl+"/orgs/"+sbConfig.IncludeOrgs[i]+"/repos", path_directory)
		if err != nil {
			return
		}
	}

	time.Sleep(time.Second * 30)

	fromdatTotodate := "1970-01-10T00:00:00TO" + last_run_copy_date[0] + "T" + last_run_copy_date[1]
	querystring := fmt.Sprintf("%s?facet=on&sort=indexdate&sortdir=des&cname=%s&query=*&facet.field=indexdate&f.indexdate.range=[%s]&f.indexdate.filter=[%s]&page=1&pagesize=1000&xsl=json", sbConfig.ServletUrl, sbConfig.Colname, fromdatTotodate, fromdatTotodate)
	reqlog.Println("url formed is:", querystring)
	resp, err := http.Get(querystring)
	if err != nil {
		errLog.Println("error occured", err)
		return
	}
	bodyBytes, _ := ioutil.ReadAll(resp.Body)
	body := string(bodyBytes)

	reqlog.Println("response is %s\n", body)
	err = json.Unmarshal(bodyBytes, &refresh_content)
	if err != nil {
		err1 := json.Unmarshal(bodyBytes, &refresh_content_single)
		if err1 != nil {
			fmt.Println("error occured @ line no:434 ", err1)
			return
		}
		url_no_amp := strings.Replace(refresh_content_single.Results.Result.Url, "amp", "", -1)
		fmt.Println("url_no_amp", url_no_amp)

		body := sbgoclient.SBDocumentRequest{
			APIKey: sbConfig.APIKey,
			Document: sbgoclient.SDocument{
				Colname: sbConfig.Colname,
				//Location: url_no_amp,
				URL: url_no_amp,
			},
		}
		fmt.Println("body is:", body)
		status_code := sbgoclient.DeleteLocalDocument(body, sbConfig)
		if status_code == 200 {
			logInfo.Info("deleted document is: ", refresh_content_single.Results.Result.Title)
		} else {
			logInfo.Info("document could not be deleted: ", refresh_content_single.Results.Result.Title)

		}
	} else {
		for key, _ := range refresh_content.Results.Result {
			url_no_amp := strings.Replace(refresh_content.Results.Result[key].Url, "amp", "", -1)
			fmt.Println("url_no_amp", url_no_amp)
			body := sbgoclient.SBDocumentRequest{
				APIKey: sbConfig.APIKey,
				Document: sbgoclient.SDocument{
					Colname: sbConfig.Colname,
					//Location: url_no_amp,
					URL: url_no_amp,
				},
			}
			reqlog.Println("body is:", body)
			status_code := sbgoclient.DeleteLocalDocument(body, sbConfig)
			if status_code == 200 {
				logInfo.Info("deleted document is: ", refresh_content.Results.Result[key].Title)
			} else {
				logInfo.Info("document could not be deleted: ", refresh_content.Results.Result[key].Title)

			}
		}
	}

	//fmt.Println("clearing static folder......")
	//cleanup(path_directory)
	elapsed := time.Since(start)
	fmt.Printf("\n github took total %v to processing\n", elapsed)
	reqlog.Println("\n github took total %v to processing\n", elapsed)
	reqlog.Println("-------------Finished Processing------------")
	fmt.Println("-------------Finished Processing------------")
}

func getfilecontents(githubUrl, path_directory string) error {
	req, err := http.NewRequest("GET", githubUrl, nil)
	req.SetBasicAuth(sbConfig.Username, sbConfig.Password)
	req.Header.Add("Accept", "application/vnd.github.v3+json")
	cli := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		}}

	resp, err := cli.Do(req)
	if err != nil || resp.StatusCode != 200 {
		if resp.StatusCode == 401 {
			fmt.Println("UnAuthorized")
			return errors.New("UnAuthorized")
		} else {
			errLog.Println("error occurred or status code is not 200", err)
			return err
		}
	}

	bodyBytes, _ := ioutil.ReadAll(resp.Body)
	var content []*RepoDetails
	err = json.Unmarshal(bodyBytes, &content)
	if err != nil {
		fmt.Println("error while unmarshalling")
		errLog.Println("unmarshalling failed", err)
		return nil
	}
	//s := strings.Trim(content[0].ContentsUrl, "/{+path}")
	for j := 0; j < len(content); j++ {
		fmt.Println("repos nam", content[j].Name)
		if !(excludeRepo(sbConfig, content[j].Name)) {
			s := strings.TrimSuffix(content[j].ContentsUrl, "/{+path}")
			req, err := http.NewRequest("GET", s, nil)
			req.SetBasicAuth(sbConfig.Username, sbConfig.Password)
			req.Header.Add("Accept", "application/vnd.github.v3+json")
			cli := &http.Client{
				CheckRedirect: func(req *http.Request, via []*http.Request) error {
					return http.ErrUseLastResponse
				}}
			resp, err := cli.Do(req)
			if err != nil || resp.StatusCode != 200 {
				if resp.StatusCode == 401 {
					fmt.Println("UnAuthorized")
					return errors.New("UnAuthorized")
				} else {
					errLog.Println("error occurred or status code is not 200", err)
					return err
				}
			}

			bodyBytes, _ := ioutil.ReadAll(resp.Body)
			var contentint []*Contents
			err = json.Unmarshal(bodyBytes, &contentint)
			if err != nil {
				fmt.Println("error while unmarshalling")
				errLog.Println("unmarshalling failed", err)
				return nil
			}

			for k := 0; k < len(contentint); k++ {
				if contentint[k].Type == "file" {
					err := github_folderlooping(true, contentint[k].Name, contentint[k].DownloadUrl, path_directory)
					if err != nil {
						return err
					}

				} else {
					//send req to traverse folder
					err := github_folderlooping(false, contentint[k].Name, s+"/"+contentint[k].Path, path_directory)
					if err != nil {
						return err
					}
				}
			}
		} else {
			fmt.Println("Repo excluded", content[j].Name)
		}
	}
	return nil

}

//Deleting static folder
func cleanup(path string) {
	err := os.RemoveAll(path)
	if err != nil {
		errLog.Println("error while clearing static folder", err)
	}
}

func github_folderlooping(isFile bool, fileName string, url string, path_directory string) error {
	reqlog.Println("github_folderlooping")
	if isFile {
		reqlog.Println("file with name ", fileName, " found")
		extension := strings.ToLower(filepath.Ext(fileName))
		isExtenstionExclude := isExcludedExtenstion(sbConfig, extension)
		isFileExclude := isExcludedFile(sbConfig, fileName)
		if !(isFileExclude) {
			if !(isExtenstionExclude) {
				req, err := http.NewRequest("GET", url, nil)
				req.SetBasicAuth(sbConfig.Username, sbConfig.Password)
				cli := &http.Client{}
				resp, err := cli.Do(req)
				if err != nil || resp.StatusCode != 200 {
					if resp.StatusCode == 401 {
						fmt.Println("UnAuthorized")
						return errors.New("UnAuthorized")
					} else {
						errLog.Println("error occurred ", err)
						return err
					}
				}

				out, err := os.Create(path_directory + "/" + fileName)
				if err != nil {
					errLog.Println("error is", err)
					return err
				}

				defer out.Close()

				// Write the body to file
				size, err := io.Copy(out, resp.Body)
				if err != nil {
					errLog.Println("error is", err)
				}
				meta := map[string]string{
					"size": strconv.FormatInt(size, 10),
					"name": fileName,
				}
				body := sbgoclient.SBDocumentRequest{
					APIKey: sbConfig.APIKey,
					Document: sbgoclient.SDocument{
						Colname:  sbConfig.Colname,
						Location: out.Name(),
						URL:      url,
						Uid:      url,
						Title:    fileName,
						Meta:     meta,
					},
				}
				reqlog.Println("body is:", body)
				statusC, _ := sbgoclient.IndexLocalDocumentGithub(body, sbConfig)
				if statusC == -1 {
					fmt.Println("the server is not connecting")
					return errors.New("the server is not connecting")
				} else if statusC == 601 {
					fmt.Println("API Key is not valid")
					return errors.New("API Key is not valid")
				} else if statusC == 501 {
					fmt.Println("Collection with name " + sbConfig.Colname + " is not present")
					return errors.New("Collection with name " + sbConfig.Colname + " is not present")
				}
			}

		} else {
			reqlog.Println("files excluded for file extension", fileName)
		}
	} else {
		reqlog.Println("folder with name ", fileName, " found")
		req, err := http.NewRequest("GET", url, nil)
		req.SetBasicAuth(sbConfig.Username, sbConfig.Password)
		cli := &http.Client{}
		resp, err := cli.Do(req)
		if err != nil || resp.StatusCode != 200 {
			if resp.StatusCode == 401 {
				fmt.Println("UnAuthorized")
				return errors.New("UnAuthorized")
			} else {
				errLog.Println("error occurred or status code is not 200", err)
				return err
			}
		}
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		var content []*Contents
		err = json.Unmarshal(bodyBytes, &content)
		if err != nil {
			errLog.Println("unmrshalling failed", err)
			return err
		}
		for i := 0; i < len(content); i++ {
			if content[i].Type == "file" {
				err := github_folderlooping(true, content[i].Name, content[i].DownloadUrl, path_directory)
				if err != nil {
					return err
				}

			} else {
				//send req to traverse folder
				err := github_folderlooping(false, content[i].Name, url+"/"+content[i].Name, path_directory)
				if err != nil {
					return err
				}

			}

		}
	}
	return nil
}

//Check if the given extension is in exclude
func isExcludedFile(config sbgoclient.SBConnectorConfig, fileExt string) bool {
	for i := 0; i < len(config.ExcludeFile); i++ {
		value := config.ExcludeFile[i]
		if value == fileExt || strings.Contains(fileExt, "LICENSE") {
			return true
		}
	}
	return false
}

//Check if the given extension is in exclude
func isExcludedExtenstion(config sbgoclient.SBConnectorConfig, fileExt string) bool {
	for i := 0; i < len(config.ExcludeFormat); i++ {
		value := config.ExcludeFormat[i]
		if value == fileExt {
			return true
		}
	}
	return false
}

//Check if the given extension is in exclude
func excludeRepo(config sbgoclient.SBConnectorConfig, repoName string) bool {
	for i := 0; i < len(config.ExcludeRepos); i++ {
		value := config.ExcludeRepos[i]
		if value == repoName {
			return true
		}
	}
	return false
}
