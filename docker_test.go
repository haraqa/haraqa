// +build !linux

package haraqa_test

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"
)

func TestDocker(t *testing.T) {
	check := func(err error) {
		if err != nil {
			panic(err)
		}
	}

	// build docker image with tests
	cmd := exec.Command("docker", "build", "-t", "haraqa-test", "--target", "compiler", "--build-arg", "TEST_ONLY=true", "--build-arg", "CACHEBUST="+strconv.FormatInt(time.Now().Unix(), 10), ".")

	// log output to console
	stdOut, err := cmd.StdoutPipe()
	check(err)
	stdErr, err := cmd.StderrPipe()
	check(err)
	check(cmd.Start())
	reader := bufio.NewReader(io.MultiReader(stdOut, stdErr))
	for {
		line, _, err := reader.ReadLine()
		if err != nil {
			break
		}
		fmt.Println(string(line))
	}
	check(cmd.Wait())

	// copy coverage and profiles to local folder
	id, err := exec.Command("docker", "create", "haraqa-test").CombinedOutput()
	check(err)
	pwd, err := os.Getwd()
	check(err)
	check(exec.Command("docker", "cp", string(id[:12])+":/profiles", pwd).Run())
	check(exec.Command("docker", "rm", string(id[:12])).Run())
}
