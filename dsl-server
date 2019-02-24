package mydsl

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-chi/chi"
	"github.com/gorilla/websocket"
	"github.com/mongodb/mongo-go-driver/bson/primitive"
	"html/template"
	_ "io"
	"log"
	"net/http"
	_ "reflect"
	"regexp"
	"strings"
)

var upgrader = websocket.Upgrader{}

var templateFuncs = template.FuncMap{
	"nl2brAndNbsp": func(text string) template.HTML {
		return template.HTML(strings.Replace(strings.Replace(template.HTMLEscapeString(text), "\n", "<br>", -1), " ", "&nbsp;", -1))
	},
	"objectIdToHex": func(any primitive.ObjectID) string {
		return any.Hex()
	},
}

func init() {
	DslAvailableFunctions["chi.NewRouter"] = chi.NewRouter
	DslAvailableFunctions["chi.URLParam"] = chi.URLParam
	DslAvailableFunctions["http.ListenAndServe"] = http.ListenAndServe

	DslFunctions["wsHandler"] = func(container map[string]interface{}, args ...Argument) (interface{}, error) {
		mux := container["router"].(*chi.Mux)

		mux.Get(args[0].rawArg.(string), func(w http.ResponseWriter, r *http.Request) {
			c, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				log.Print("upgrade:", err)
				return
			}
			newContainer := map[string]interface{}{"conn": c}
			for {
				_, message, err := c.ReadMessage()
				if err != nil {
					log.Println("read:", err)
					break
				}
				var data interface{}
				err = json.Unmarshal(message, &data)
				newContainer["message"] = data
				if err != nil {
					fmt.Println("unmarshal error", err, data)
					break
				}
				args[1].Evaluate(newContainer)
			}
			defer func() {
				c.Close()
				args[2].Evaluate(newContainer)
			}()

		})
		return nil, nil
	}

	DslFunctions["wsWrite"] = func(container map[string]interface{}, args ...Argument) (interface{}, error) {
		conn := container["conn"].(*websocket.Conn)
		evaluated, err := args[0].Evaluate(container)
		if err != nil {
			return nil, err
		}
		b, err := json.Marshal(evaluated)
		err = conn.WriteMessage(1, []byte(b))
		if err != nil {
			log.Println("write:", err)
			return nil, err
		}
		return nil, nil
	}

	DslFunctions["handler"] = func(container map[string]interface{}, args ...Argument) (interface{}, error) {
		method := args[0].rawArg
		endpoint := args[1].rawArg
		viewOrLogic := args[2].rawArg
		if _, ok := viewOrLogic.(string); ok {
			return nil, nil // TBD
		} else {
			if method == "get" {
				(container["router"].(*chi.Mux)).Get(endpoint.(string), func(res http.ResponseWriter, req *http.Request) {
					newContainer := map[string]interface{}{"req": req, "res": res}
					args[2].Evaluate(newContainer)
				})
				return nil, nil
			} else {
				(container["router"].(*chi.Mux)).Post(endpoint.(string), func(res http.ResponseWriter, req *http.Request) {
					newContainer := map[string]interface{}{"req": req, "res": res}
					args[2].Evaluate(newContainer)
				})
				return nil, nil // TBD
			}
		}
	}

	DslFunctions["send"] = func(container map[string]interface{}, args ...Argument) (interface{}, error) {
		evaluated, err := args[0].Evaluate(container)
		if err != nil {
			return nil, err
		}
		(container["res"].(http.ResponseWriter)).Write([]byte(evaluated.(string))) // TBD
		return nil, nil
	}

	DslFunctions["render"] = func(container map[string]interface{}, args ...Argument) (interface{}, error) {
		evaluated, err := args[0].Evaluate(container)
		templateArgument, err := args[1].Evaluate(container)
		if err != nil {
			return nil, err
		}
		t, err := template.New("titleTest").Funcs(templateFuncs).ParseFiles("templates/" + evaluated.(string))
		if err := t.ExecuteTemplate((container["res"].(http.ResponseWriter)), evaluated.(string), templateArgument); err != nil {
			// log.Fatal(err)
		}
		return nil, nil
	}
	DslFunctions["redirect"] = func(container map[string]interface{}, args ...Argument) (interface{}, error) {
		toRedirect := args[0].rawArg.(string)
		http.Redirect((container["res"].(http.ResponseWriter)), (container["req"].(*http.Request)), toRedirect, http.StatusMovedPermanently)
		return nil, nil
	}

	var processes = map[string]chan int{}
	var processIdPattern = regexp.MustCompile(`^(.+)(\d{13})$`)
	DslFunctions["processStart"] = func(container map[string]interface{}, args ...Argument) (interface{}, error) {
		processId, err := args[0].Evaluate(container)
		if err != nil {
			return nil, err
		} else {
			fmt.Println("process start", processId.(string), args[1])
			dsl, err := args[1].Evaluate(container)
			if err != nil {
				return nil, err
			}
			gochan := make(chan int)
			go func() {
				result, err := NewArgument(dsl).Evaluate(map[string]interface{}{})
				if err == nil {
					if typedResult, ok := result.(chan int); ok {
						processes[processId.(string)] = typedResult
						fmt.Println("process start result", result)
					} else {
						fmt.Println("no channel returned.")
					}
				}
				gochan <- 1
			}()
			<-gochan
		}
		return nil, nil
	}
	DslFunctions["processKill"] = func(container map[string]interface{}, args ...Argument) (interface{}, error) {
		processId, err := args[0].Evaluate(container)
		if err != nil {
			return nil, err
		}
		if processId == nil {
			return nil, nil
		}

		channel, ok := processes[processId.(string)]
		if !ok {
			return nil, errors.New("channel not found")
		}
		channel <- 0
		close(channel)
		delete(processes, processId.(string))
		return nil, nil
	}

	DslFunctions["processes"] = func(container map[string]interface{}, args ...Argument) (interface{}, error) {
		result := map[interface{}][]string{}
		for key, _ := range processes {
			match := processIdPattern.FindStringSubmatch(key)
			yamlId := match[1]
			if slice, ok := result[yamlId]; ok {
				slice = append(slice, match[2])
				result[yamlId] = slice
			} else {
				result[yamlId] = []string{match[2]}
			}
		}
		//fmt.Println("processes...", result)
		//result["5c40351e93ac4c189d09d789"] = []string{"111111"}
		return result, nil
	}

	pubsubChannels := map[string][]chan interface{}{}

	DslFunctions["subscribe"] = func(container map[string]interface{}, args ...Argument) (interface{}, error) {
		exitChannel := make(chan int)
		channel := make(chan interface{})
		evaluated, err := args[0].Evaluate(container)
		if err != nil {
			return nil, err
		}
		channelName, ok := evaluated.(string)
		if !ok {
			return nil, errors.New(fmt.Sprintf("subscribe channel name must be string. %v", channelName))
		}
		go func() {
			for {
				select {
				case data := <-channel:
					newContainer := map[string]interface{}{"subscribe": data, "channelName": channelName}
					if len(args) > 2 {
						for _, key := range args[2].rawArg.([]interface{}) {
							newContainer[key.(string)] = container[key.(string)]
						}
					}
					args[1].Evaluate(newContainer)
				case <-exitChannel:
					channels := pubsubChannels[channelName]
					removed := []chan interface{}{}
					for _, ch := range channels {
						if ch != channel {
							removed = append(removed, ch)
						}
					}
					pubsubChannels[channelName] = removed
					close(channel)
					fmt.Println("channel closed", pubsubChannels)
					return
				}
			}
		}()
		if channels, ok := pubsubChannels[channelName]; ok {
			channels = append(channels, channel)
			pubsubChannels[channelName] = channels
		} else {
			pubsubChannels[channelName] = []chan interface{}{channel}
			// TBD
			if channelName != "channelList" {
				NewArgument(map[interface{}]interface{}{
					"publish": []interface{}{
						"channelList",
						map[interface{}]interface{}{"channelList": nil},
					},
				}).Evaluate(map[string]interface{}{})
			}
		}
		fmt.Println("add subscribe channels", pubsubChannels)
		return exitChannel, nil
	}

	DslFunctions["publish"] = func(container map[string]interface{}, args ...Argument) (interface{}, error) {
		channelName, err := args[0].Evaluate(container)
		if err != nil {
			return nil, err
		}
		typedChannelName, ok := channelName.(string)
		if !ok {
			return nil, errors.New(fmt.Sprintf("publish channel name must be string. %v", channelName))
		}
		evaluated, err := args[1].Evaluate(container)
		if err != nil {
			return nil, err
		}
		if channels, ok := pubsubChannels[typedChannelName]; ok {
			for _, channel := range channels {
				go func(ch chan interface{}) {
					ch <- evaluated
				}(channel)
			}
		} else {
			fmt.Println(fmt.Sprintf("channel: %v has no subscribers.", typedChannelName))
			pubsubChannels[typedChannelName] = []chan interface{}{}
			// TBD
			if typedChannelName != "channelList" {
				NewArgument(map[interface{}]interface{}{
					"publish": []interface{}{
						"channelList",
						map[interface{}]interface{}{"channelList": nil},
					},
				}).Evaluate(map[string]interface{}{})
			}
		}
		return nil, nil
	}
	DslFunctions["channelList"] = func(container map[string]interface{}, args ...Argument) (interface{}, error) {
		result := []string{}
		for key, _ := range pubsubChannels {
			result = append(result, key)
		}
		return result, nil
	}
}
