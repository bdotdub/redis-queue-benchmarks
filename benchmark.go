package main

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
)

const (
	apnsJSONTemplate = `{"token":"%10d9999999999999999%5d999999999999999999999999999999999","badge":1,content_available:0,"body":"Your Timehop day is ready!\nView it now →"}`
	benchmarkRedisDB = "15"
)

type Msgs struct {
	msgs []string
	t    time.Time
}

type Result struct {
	Add         time.Duration
	AddedItems  int
	Pop         time.Duration
	PoppedItems int
	Memory      string
}

func msgs() *[]Msgs {
	ms := []Msgs{}

	tz, _ := time.LoadLocation("America/New_York")
	t := time.Date(2014, 1, 17, 4, 0, 0, 0, tz)

	for hr := 0; hr < 1; hr++ {
		for min := 15; min < 45; min++ {
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

func memoryHuman(r redis.Conn) string {
	s, _ := redis.String(r.Do("INFO", "memory"))
	for _, l := range strings.Split(s, "\n") {
		if strings.HasPrefix(l, "used_memory_human") {
			parts := strings.Split(l, ":")
			return parts[1]
		}
	}

	return ""
}

func config(r redis.Conn, c string) string {
	s, _ := redis.Strings(r.Do("config", "get", c))
	return s[1]
}

func justZset(r redis.Conn, ms *[]Msgs) Result {
	res := Result{}

	tAdd := time.Now()
	for _, m := range *ms {
		for _, s := range m.msgs {
			r.Do("ZADD", "apns:push:delayed", fmt.Sprint(m.t.Unix()), s)
			res.AddedItems += 1
		}
	}
	res.Add = time.Since(tAdd)
	res.Memory = memoryHuman(r)

	tPop := time.Now()
	for _, m := range *ms {
		for {
			m, _ := redis.Strings(r.Do("zrangebyscore", "apns:push:delayed", "-inf", fmt.Sprint(m.t.Unix()), "limit", 0, 1))

			if len(m) == 0 {
				break
			}

			redis.Bool(r.Do("zrem", "apns:push:delayed", m[0]))
			res.PoppedItems += 1
		}
	}

	res.Pop = time.Since(tPop)
	return res
}

func listWithZset(r redis.Conn, ms *[]Msgs) Result {
	res := Result{}

	tAdd := time.Now()
	for _, m := range *ms {
		r.Do("ZADD", "apns:push:delayed", fmt.Sprint(m.t.Unix()), fmt.Sprint(m.t.Unix()))
		for _, s := range m.msgs {
			r.Do("ZADD", "apns:push:delayed", fmt.Sprint(m.t.Unix()), fmt.Sprint(m.t.Unix()))
			r.Do("RPUSH", fmt.Sprintf("apns:push:delayed:%v", m.t.Unix()), s)

			res.AddedItems += 1
		}
	}

	res.Add = time.Since(tAdd)
	res.Memory = memoryHuman(r)

	tPop := time.Now()
	for {
		v, err := r.Do("zrangebyscore", "apns:push:delayed", "-inf", "+inf", "limit", 0, 1)

		vs, _ := redis.Values(v, err)
		if len(vs) == 0 {
			break
		}

		bs := vs[0].([]byte)
		m := string(bs)

		redis.Bool(r.Do("zrem", "apns:push:delayed", m))

		for {
			mm, _ := redis.String(r.Do("LPOP", fmt.Sprintf("apns:push:delayed:%v", m)))
			if len(mm) == 0 {
				break
			}
			res.PoppedItems += 1
		}
	}

	res.Pop = time.Since(tPop)
	return res
}

func main() {
	r, err := redis.Dial("tcp", ":6379")
	if err != nil {
		log.Fatal("couldn't connect to redis")
	}
	defer r.Close()

	r.Do("SELECT", benchmarkRedisDB)

	ms := msgs()

	fmt.Println("zset-max-ziplist-entries: ", config(r, "zset-max-ziplist-entries"))

	lwz := listWithZset(r, ms)

	fmt.Println("====================")
	fmt.Println("List with Zset")

	fmt.Printf("Added\t%v\t%v\n", lwz.AddedItems, lwz.Add)
	fmt.Printf("Popped\t%v\t%v\n", lwz.PoppedItems, lwz.Pop)
	fmt.Printf("Memory: %v\n\n", lwz.Memory)

	jz := justZset(r, ms)

	fmt.Println("====================")
	fmt.Println("Just Zset")

	fmt.Printf("Added\t%v\t%v\n", jz.AddedItems, jz.Add)
	fmt.Printf("Popped\t%v\t%v\n", jz.PoppedItems, jz.Pop)
	fmt.Printf("Memory: %v\n\n", jz.Memory)

	r.Do("flushall")
}
