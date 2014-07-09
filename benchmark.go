package main

import (
	"fmt"
	"log"
	"time"

	"github.com/garyburd/redigo/redis"
)

const (
	apnsJSONTemplate = `{"token":"%10d9999999999999999%5d999999999999999999999999999999999","badge":1,content_available:0,"body":"Your Timehop day is ready!\nView it now →"}`
)

type Msgs struct {
	msgs []string
	t    time.Time
}

func msgs() *[]Msgs {
	ms := []Msgs{}

	tz, _ := time.LoadLocation("America/New_York")
	t := time.Date(2014, 1, 17, 4, 0, 0, 0, tz)

	for hr := 0; hr < 1; hr++ {
		for min := 15; min <= 45; min++ {
			nt := t.Add(time.Duration(hr) * time.Hour).Add(time.Duration(min) * time.Minute)

			msgs := make([]string, 33333)
			for i := 0; i < 33333; i++ {
				msgs[i] = fmt.Sprintf(apnsJSONTemplate, nt.Unix(), i)
			}

			ms = append(ms, Msgs{msgs: msgs, t: nt})
		}
	}

	return &ms
}

func justZset(r redis.Conn, ms *[]Msgs) {
	tAdd := time.Now()
	for _, m := range *ms {
		fmt.Println("Adding for", m.t)
		for _, s := range m.msgs {
			r.Do("ZADD", "apns:push:delayed", fmt.Sprint(m.t.Unix()), s)
		}
	}
	fmt.Println("Adding", time.Since(tAdd))

	tPop := time.Now()
	for _, m := range *ms {
		fmt.Println("Popping for", m.t)
		for {
			m, _ := redis.Strings(r.Do("zrangebyscore", "apns:push:delayed", "-inf", fmt.Sprint(m.t.Unix()), "limit", 0, 1))

			if len(m) == 0 {
				break
			}

			redis.Bool(r.Do("zrem", "apns:push:delayed", m[0]))
		}
	}
	fmt.Println("Popping", time.Since(tPop))
}

func main() {
	r, err := redis.Dial("tcp", ":6379")
	if err != nil {
		log.Fatal("couldn't connect to redis")
	}
	defer r.Close()

	r.Do("SELECT", "5")

	ms := msgs()

	justZset(r, ms)
}
