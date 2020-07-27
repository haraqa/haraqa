package server_test

/*
func TestBrokerHTTP(t *testing.T) {
	b := make([]byte, 10)
	rand.Read(b)
	randomName := base64.URLEncoding.EncodeToString(b)
	dirNames := []string{
		".haraqa1-" + randomName,
		".haraqa2-" + randomName,
		".haraqa3-" + randomName,
	}
	defer func() {
		for _, name := range dirNames {
			os.RemoveAll(name)
		}
	}()

	haraqaServer, err := server.NewServer(
		server.WithDirs(dirNames...),
	)
	if err != nil {
		t.Fatal(err)
	}

	s := httptest.NewServer(haraqaServer)
	defer s.Close()

	err = client.CreateTopic(s.Client(), s.URL, "testtopic")
	if err != nil {
		t.Fatal(err)
	}

	err = client.Produce(s.Client(), s.URL, "testtopic", [][]byte{
		[]byte("something"),
		[]byte("something else"),
	})
	if err != nil {
		t.Fatal(err)
	}

	resp, err := s.Client().Get(s.URL + "/topics/testtopic/1")
	if err != nil {
		t.Fatal(err)
	}
	body, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "something else" {
		t.Fatal(string(body))
	}
}

func BenchmarkProduce(b *testing.B) {
	rnd := make([]byte, 10)
	rand.Read(rnd)
	randomName := base64.URLEncoding.EncodeToString(rnd)
	dirNames := []string{
		".haraqa1-" + randomName,
		".haraqa2-" + randomName,
	}
	defer func() {
		for _, name := range dirNames {
			os.RemoveAll(name)
		}
	}()

	haraqaServer, err := server.NewServer(
		server.WithDirs(dirNames...),
	)
	if err != nil {
		b.Fatal(err)
	}

	s := httptest.NewServer(haraqaServer)
	defer s.Close()

	err = client.CreateTopic(s.Client(), s.URL, "benchtopic")
	if err != nil {
		b.Fatal(err)
	}

	msgs := make([][100]byte, 100)
	sizes := make([]string, len(msgs))
	var data []byte
	for i := range msgs {
		copy(msgs[i][:], []byte("something"))
		data = append(data, msgs[i][:]...)
		sizes[i] = strconv.Itoa(len(msgs[i]))
	}
	xSizes := strings.Join(sizes, ",")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i += len(msgs) {
		body := bytes.NewBuffer(data)
		req, err := http.NewRequest("POST", "/topics/benchtopic", body)
		if err != nil {
			b.Fatal(err)
		}
		req.Header.Set("X-SIZES", xSizes)
		recorder := httptest.NewRecorder()
		haraqaServer.ServeHTTP(recorder, req)
		if recorder.Result().StatusCode != http.StatusOK {
			b.Fatal(recorder.Result().StatusCode)
		}
	}
}

func BenchmarkConsume(b *testing.B) {
	rnd := make([]byte, 12)
	rand.Read(rnd)
	randomName := base64.URLEncoding.EncodeToString(rnd)
	dirNames := []string{
		".haraqa1-" + randomName,
	}
	defer func() {
		for _, name := range dirNames {
			os.RemoveAll(name)
		}
	}()

	haraqaServer, err := server.NewServer(
		server.WithDirs(dirNames...),
	)
	if err != nil {
		b.Fatal(err)
	}

	s := httptest.NewServer(haraqaServer)
	defer s.Close()

	err = client.CreateTopic(s.Client(), s.URL, "benchtopic")
	if err != nil {
		b.Fatal(err)
	}

	msgs := make([][100]byte, 100)
	sizes := make([]string, len(msgs))
	var data []byte
	for i := range msgs {
		copy(msgs[i][:], []byte("something"))
		data = append(data, msgs[i][:]...)
		sizes[i] = strconv.Itoa(len(msgs[i]))
	}
	xSizes := strings.Join(sizes, ",")

	for i := 0; i < b.N; i += len(msgs) {
		body := bytes.NewBuffer(data)
		req, err := http.NewRequest("POST", "/topics/benchtopic", body)
		if err != nil {
			b.Fatal(err)
		}
		req.Header.Set("X-SIZES", xSizes)
		recorder := httptest.NewRecorder()
		haraqaServer.ServeHTTP(recorder, req)
		if recorder.Result().StatusCode != http.StatusOK {
			b.Fatal(recorder.Result().StatusCode)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; {

		req, err := http.NewRequest("GET", "/topics/benchtopic/0", nil)
		if err != nil {
			b.Fatal(err)
		}
		req.Header.Set("X-BATCH-SIZE", "1000")
		recorder := httptest.NewRecorder()
		haraqaServer.ServeHTTP(recorder, req)
		body, err := ioutil.ReadAll(recorder.Result().Body)
		if err != nil {
			b.Fatal(err)
		}

		b.Log(b.N, i, recorder.Result().StatusCode, len(body), req.Header)

		i += len(body) / 100
	}
}
*/
