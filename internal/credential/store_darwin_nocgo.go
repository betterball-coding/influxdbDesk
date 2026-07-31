//go:build darwin && !cgo

package credential

type Store struct{}

func NewStore() *Store { return &Store{} }

func (*Store) Put(string, Kind, []byte) error { return ErrUnavailable }

func (*Store) Get(string, Kind) ([]byte, error) { return nil, ErrUnavailable }

func (*Store) Delete(string, Kind) error { return ErrUnavailable }

func (*Store) DeleteProfile(string) error { return ErrUnavailable }
