package nullwriter

type NW struct{}

func New() NW {
	return NW{}
}

func (NW) WriteAt(p []byte, off int64) (n int, err error) {
	return len(p), nil
}
