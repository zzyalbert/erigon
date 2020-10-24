package filters

type Filter interface {
	Add([]byte)
	Test([]byte) bool
	Name() string
}

type Deleter interface {
	Delete([]byte)
}
