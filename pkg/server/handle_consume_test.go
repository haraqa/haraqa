package server

/*
// go:generate mockgen -package server -destination handle_consume_mocks_test.go io ReadSeeker
// go:generate goimports -w handle_consume_mocks_test.go

type readSeekCloser struct {
	io.ReadSeeker
}

func (rsc readSeekCloser) Close() error { return nil }

func TestServer_HandleConsume(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	topic := "consume_topic"
	q := NewMockQueue(ctrl)
	rs := NewMockReadSeeker(ctrl)
	info := protocol.ConsumeInfo{
		Filename:  "test_file",
		File:      readSeekCloser{rs},
		Exists:    true,
		StartAt:   20,
		EndAt:     50,
		StartTime: time.Now().Add(-1 * time.Hour),
		EndTime:   time.Now(),
		Sizes:     []int64{5, 10, 15},
	}
	output := make([]byte, info.EndAt-info.StartAt)
	_, _ = rand.Read(output)
	gomock.InOrder(
		// happy path
		q.EXPECT().Consume(topic, int64(123), int64(-1)).Return(&info, nil).Times(1),
		rs.EXPECT().Seek(int64(0), io.SeekEnd).Return(int64(info.EndAt), nil).Times(1),
		rs.EXPECT().Seek(int64(0), io.SeekStart).Return(int64(0), nil).Times(1),
		rs.EXPECT().Seek(int64(info.StartAt), io.SeekStart).Return(int64(0), nil).Times(1),
		rs.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
			if len(b) != int(info.EndAt-info.StartAt) {
				t.Error(len(b))
			}
			copy(b, output)
			return len(b), nil
		}).Times(1),

		// empty file
		q.EXPECT().Consume(topic, int64(123), int64(-1)).Return(&info, nil).Times(1),
		rs.EXPECT().Seek(int64(0), io.SeekEnd).Return(int64(0), nil).Times(1),
		rs.EXPECT().Seek(int64(0), io.SeekStart).Return(int64(0), nil).Times(1),

		q.EXPECT().Consume(topic, int64(123), int64(-1)).Return(&protocol.ConsumeInfo{Exists: false}, nil).Times(1),
		q.EXPECT().Consume(topic, int64(123), int64(-1)).Return(nil, protocol.ErrTopicDoesNotExist).Times(1),
		q.EXPECT().Consume(topic, int64(123), int64(-1)).Return(nil, errors.New("test consume error")).Times(1),
	)
	s := Server{q: q, metrics: noOpMetrics{}, defaultLimit: -1}

	// invalid topic
	{
		w := httptest.NewRecorder()
		r, err := http.NewRequest(http.MethodGet, "/topics/", bytes.NewBuffer([]byte("test body")))
		if err != nil {
			t.Fatal(err)
		}

		s.HandleConsume()(w, r)
		resp := w.Result()
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatal(resp.Status)
		}
		err = protocol.ReadErrors(resp.Header)
		if err != protocol.ErrInvalidTopic {
			t.Fatal(err)
		}
	}

	// valid topic, invalid id
	{
		w := httptest.NewRecorder()
		r, err := http.NewRequest(http.MethodGet, "/topics/", bytes.NewBuffer([]byte("test body")))
		if err != nil {
			t.Fatal(err)
		}
		r = mux.SetURLVars(r, map[string]string{"topic": topic, "id": "invalid"})

		s.HandleConsume()(w, r)
		resp := w.Result()
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatal(resp.Status)
		}
		err = protocol.ReadErrors(resp.Header)
		if err != protocol.ErrInvalidMessageID {
			t.Fatal(err)
		}
	}

	// valid topic, valid id, invalid limit
	{
		w := httptest.NewRecorder()
		r, err := http.NewRequest(http.MethodGet, "/topics/"+topic, bytes.NewBuffer([]byte("test body")))
		if err != nil {
			t.Fatal(err)
		}
		r = mux.SetURLVars(r, map[string]string{"topic": topic, "id": "123"})
		r.Header.Set(protocol.HeaderLimit, "invalid")
		s.HandleConsume()(w, r)
		resp := w.Result()
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatal(resp.Status)
		}
		err = protocol.ReadErrors(resp.Header)
		if err != protocol.ErrInvalidHeaderLimit {
			t.Fatal(err)
		}
	}

	// valid topic, valid id, happy path
	{
		w := httptest.NewRecorder()
		r, err := http.NewRequest(http.MethodGet, "/topics/"+topic, bytes.NewBuffer([]byte("Hello World")))
		if err != nil {
			t.Fatal(err)
		}
		r = mux.SetURLVars(r, map[string]string{"topic": topic, "id": "123"})

		s.HandleConsume()(w, r)
		resp := w.Result()
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusPartialContent {
			t.Fatal(resp.Status)
		}
		err = protocol.ReadErrors(resp.Header)
		if err != nil {
			t.Fatal(err)
		}
		if resp.Header.Get(protocol.HeaderStartTime) != info.StartTime.Format(time.ANSIC) ||
			resp.Header.Get(protocol.HeaderEndTime) != info.EndTime.Format(time.ANSIC) ||
			resp.Header.Get(protocol.HeaderFileName) != info.Filename ||
			resp.Header.Get("Content-Type") != "application/octet-stream" {
			t.Fatal(resp.Header)
		}
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(b, output) {
			t.Fatal("bytes not equal", b, output)
		}
	}

	// valid topic, valid id, empty file
	{
		w := httptest.NewRecorder()
		r, err := http.NewRequest(http.MethodGet, "/topics/"+topic, bytes.NewBuffer([]byte("Hello World")))
		if err != nil {
			t.Fatal(err)
		}
		r = mux.SetURLVars(r, map[string]string{"topic": topic, "id": "123"})

		s.HandleConsume()(w, r)
		resp := w.Result()
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusRequestedRangeNotSatisfiable {
			t.Fatal(resp.Status)
		}
	}

	// valid topic, valid id, no content
	{
		w := httptest.NewRecorder()
		r, err := http.NewRequest(http.MethodGet, "/topics/"+topic, bytes.NewBuffer([]byte("Hello World")))
		if err != nil {
			t.Fatal(err)
		}
		r = mux.SetURLVars(r, map[string]string{"topic": topic, "id": "123"})

		s.HandleConsume()(w, r)
		resp := w.Result()
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			t.Fatal(resp.Status)
		}
		err = protocol.ReadErrors(resp.Header)
		if err != protocol.ErrNoContent {
			t.Fatal(err)
		}
	}

	// valid topic, queue error: topic does not exist
	{
		w := httptest.NewRecorder()
		r, err := http.NewRequest(http.MethodGet, "/topics/"+topic, bytes.NewBuffer([]byte("Hello World")))
		if err != nil {
			t.Fatal(err)
		}
		r = mux.SetURLVars(r, map[string]string{"topic": topic, "id": "123"})

		s.HandleConsume()(w, r)
		resp := w.Result()
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusPreconditionFailed {
			t.Fatal(resp.Status)
		}
		err = protocol.ReadErrors(resp.Header)
		if err != protocol.ErrTopicDoesNotExist {
			t.Fatal(err)
		}
	}

	// valid topic, queue error: unknown error
	{
		w := httptest.NewRecorder()
		r, err := http.NewRequest(http.MethodGet, "/topics/"+topic, bytes.NewBuffer([]byte("Hello World")))
		if err != nil {
			t.Fatal(err)
		}
		r = mux.SetURLVars(r, map[string]string{"topic": topic, "id": "123"})

		s.HandleConsume()(w, r)
		resp := w.Result()
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusInternalServerError {
			t.Fatal(resp.Status)
		}
		err = protocol.ReadErrors(resp.Header)
		if err.Error() != "test consume error" {
			t.Fatal(err)
		}
	}
}
*/
