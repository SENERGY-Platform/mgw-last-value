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

package storage

import (
	"context"
	"github.com/SENERGY-Platform/mgw-last-value/pkg/configuration"
	"github.com/SENERGY-Platform/mgw-last-value/pkg/storage/badger"
	"github.com/SENERGY-Platform/mgw-last-value/pkg/storage/bolt"
	"runtime"
	"sync"
	"time"
)

type Storage interface {
	Set(key string, value []byte) error
	Get(key string) (value []byte, time *time.Time, err error)
}

func NewWithConfig(ctx context.Context, wg *sync.WaitGroup, config configuration.Config) (result Storage, err error) {
	switch config.StorageSelection {
	case "bolt":
		return bolt.NewWithConfig(ctx, wg, config)
	case "badger":
		return badger.NewWithConfig(ctx, wg, config)
	case "auto":
		if runtime.GOARCH == "arm" {
			return bolt.NewWithConfig(ctx, wg, config)
		} else {
			return badger.NewWithConfig(ctx, wg, config)
		}
	default:
		return badger.NewWithConfig(ctx, wg, config)
	}
}
