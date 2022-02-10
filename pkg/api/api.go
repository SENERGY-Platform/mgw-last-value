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
	"context"
	"errors"
	"fmt"
	"github.com/SENERGY-Platform/mgw-last-value/pkg/api/util"
	"github.com/SENERGY-Platform/mgw-last-value/pkg/configuration"
	"github.com/julienschmidt/httprouter"
	"log"
	"net/http"
	"reflect"
	"runtime"
	"runtime/debug"
	"sync"
	"time"
)

type Getter interface {
	Get(deviceKey, serviceKey, path string) (value interface{}, time *time.Time, err error)
}

var endpoints = []func(config configuration.Config, router *httprouter.Router, getter Getter){}

func Start(ctx context.Context, wg *sync.WaitGroup, config configuration.Config, getter Getter) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New(fmt.Sprint(r))
		}
	}()
	router := GetRouter(config, getter)

	server := &http.Server{Addr: ":" + config.HttpPort, Handler: router}
	go func() {
		log.Println("listening on ", server.Addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			debug.PrintStack()
			log.Fatal("FATAL:", err)
		}
	}()
	if wg != nil {
		wg.Add(1)
	}
	go func() {
		defer wg.Done()
		<-ctx.Done()
		log.Println("api shutdown", server.Shutdown(context.Background()))
	}()
	return
}

func GetRouter(config configuration.Config, getter Getter) http.Handler {
	router := httprouter.New()
	for _, e := range endpoints {
		log.Println("add endpoint: " + runtime.FuncForPC(reflect.ValueOf(e).Pointer()).Name())
		e(config, router, getter)
	}
	handler := util.NewCors(router)
	handler = util.NewLogger(handler)
	return handler
}
