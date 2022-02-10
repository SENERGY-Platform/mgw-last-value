/*
 * Copyright 2022 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package api

import (
	"encoding/json"
	"github.com/SENERGY-Platform/mgw-last-value/pkg/configuration"
	"github.com/julienschmidt/httprouter"
	"net/http"
	"time"
)

func init() {
	endpoints = append(endpoints, LastValueEndpoint)
}

func LastValueEndpoint(config configuration.Config, router *httprouter.Router, getter Getter) {
	resource := "/last-values"

	router.POST(resource, func(writer http.ResponseWriter, request *http.Request, params httprouter.Params) {
		lastValueRequests := []LastValueRequest{}
		err := json.NewDecoder(request.Body).Decode(&lastValueRequests)
		if err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
		result := make([]LastValueResponse, len(lastValueRequests))
		for i, req := range lastValueRequests {
			var tempTime *time.Time
			result[i].Value, tempTime, err = getter.Get(req.DeviceId, req.ServiceId, req.ColumnName)
			if err != nil {
				http.Error(writer, err.Error(), http.StatusInternalServerError)
				return
			}
			if tempTime != nil {
				timeStr := tempTime.Format(time.RFC3339)
				result[i].Time = &timeStr
			}
		}
		writer.Header().Set("Content-Type", "application/json; charset=utf-8")
		json.NewEncoder(writer).Encode(result)
		return
	})
}

//similar request and response as in https://github.com/SENERGY-Platform/timescale-wrapper/blob/master/pkg/api/last-values.go

type LastValueRequest struct {
	DeviceId   string
	ServiceId  string
	ColumnName string
}

type LastValueResponse struct {
	Time  *string     `json:"time"`
	Value interface{} `json:"value"`
}
