package utils

import (
	"math/rand"
	"time"
)

// 获取一个随机数
func GetRandInt(max int) int {
	rand.Seed(time.Now().UnixNano()) // (这是个重点，后期整理一下) 这里注意：如果用Unix()的话，程序执行特别快那么前后两次的种子会一样，导致生成的随机数也一样
	return rand.Intn(max)
}
