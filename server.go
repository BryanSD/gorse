package main

import (
    "encoding/json"
    "fmt"
    "io/ioutil"
    "net/http"

    "github.com/go-martini/martini"
    "github.com/kuwagata/martini-keystone-auth"
    "github.com/powellchristoph/rsekafka"
)

func main() {
    m := martini.Classic()

    auth_handler := setupAuthHandler()
    m.Handlers(
        auth_handler,
        martini.Recovery(),
    )

    storage := rsekafka.NewClient([]string{"192.168.59.103:9092"})

    m.Get("/**", func(request *http.Request, params martini.Params, response http.ResponseWriter) string {
        topic := params["_1"]
        marker, marker_present := request.URL.Query()["marker"]

        // Get message from storage
        // if marker_present {
        //     storage.Get(topic, marker)
        // } else {
        //     storage.Get(topic)
        // }
        storage_events, _ := storage.Get(topic, 0)
        events := make([]map[string]string, 0)
        for _, event := range storage_events {
            // var valid_json interface{}
            // err = json.Unmarshal(string(event), &valid_json)
            // if err != nil {
            //     panic("Invalid JSON")
            // }

            eventMap := make(map[string]string)
            eventMap["data"] = string(event.Value)
            events = append(events, eventMap)
        }

        // Example return
        // events := make([]map[string]string, 0)
        // event := make(map[string]string)
        // event["property"] = "value"
        // events = append(events, event)
        jsonString, _ := json.Marshal(events)
        fmt.Println("Events: " + string(jsonString))

        tempReturn := "Getting messages on " + topic
        if marker_present {
            tempReturn += " with marker of " + marker[0]
        }
        // fmt.Println(tempReturn)
        // return tempReturn + "\r\n"

        response.Header()["Content-Type"] = []string{"application/json"}
        return string(jsonString) + "\r\n"
    })

    m.Post("/**", func(request *http.Request, params martini.Params, response http.ResponseWriter) {
        topic := params["_1"]

        body, err := ioutil.ReadAll(request.Body)
        if err != nil {
            panic("Unable to read body")
        }
        var valid_json interface{}
        err = json.Unmarshal(body, &valid_json)
        if err != nil {
            panic("Invalid JSON")
        }

        fmt.Println("Topic: " + topic)
        fmt.Println("Message: " + string(body))
        // Send message to storage
        storage.Post(topic, string(body))

        response.WriteHeader(http.StatusCreated)
    })

    m.Run()
}


func setupAuthHandler() martini.Handler {
    return auth.Keystone(
        auth.IdentityValidator{AuthUrl: "https://identity.api.rackspacecloud.com/v2.0/tokens"},
        auth.Redis{
            Hostname: "192.168.59.103",
            Port:     "6379",
            Password: "",
            Database: int64(0)})
}
