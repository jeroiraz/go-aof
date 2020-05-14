package aof

type gFoldHandler struct {
	f FoldFn
	v interface{}
}

func (h *gFoldHandler) Fold(e *Entry) (bool, error) {
	nv, concluded, err := h.f(e, h.v)
	if err == nil {
		h.v = nv
	}
	return concluded, err
}

func (h *gFoldHandler) Value() interface{} {
	return h.v
}

func (h *gFoldHandler) Values() []interface{} {
	return nil
}

type forEachHandler struct {
	f ForEachFn
}

func (h *forEachHandler) Fold(e *Entry) (bool, error) {
	return h.f(e)
}

func (h *forEachHandler) Value() interface{} {
	return nil
}

func (h *forEachHandler) Values() []interface{} {
	return nil
}

type filterHandler struct {
	f  FilterFn
	ls []*Entry
}

func (h *filterHandler) Fold(e *Entry) (bool, error) {
	v, concluded, err := h.f(e)
	if err == nil && v {
		h.ls = append(h.ls, e)
	}
	return concluded, err
}

func (h *filterHandler) Value() interface{} {
	return nil
}

func (h *filterHandler) Values() []interface{} {
	ls := make([]interface{}, len(h.ls))
	for i, e := range h.ls {
		ls[i] = e
	}
	return ls
}

type mapHandler struct {
	f  MapFn
	ls []interface{}
}

func (h *mapHandler) Fold(e *Entry) (bool, error) {
	l, concluded, err := h.f(e)
	if err == nil {
		h.ls = append(h.ls, l)
	}
	return concluded, err
}

func (h *mapHandler) Value() interface{} {
	return nil
}

func (h *mapHandler) Values() []interface{} {
	ls := make([]interface{}, len(h.ls))
	for i, e := range h.ls {
		ls[i] = e
	}
	return ls
}

type filteredMapHandler struct {
	f  FilterFn
	m  MapFn
	ls []interface{}
}

func (h *filteredMapHandler) Fold(e *Entry) (bool, error) {
	v, concludedf, err := h.f(e)
	if err == nil && v {
		l, concludedm, err := h.m(e)
		if err == nil {
			h.ls = append(h.ls, l)
		}
		return concludedf || concludedm, err
	}
	return concludedf, err
}

func (h *filteredMapHandler) Value() interface{} {
	return nil
}

func (h *filteredMapHandler) Values() []interface{} {
	ls := make([]interface{}, len(h.ls))
	for i, e := range h.ls {
		ls[i] = e
	}
	return ls
}

type sizeFoldHandler struct {
	app  *Appender
	size int64
}

func (h *sizeFoldHandler) Fold(e *Entry) (bool, error) {
	if e.size > 0 && !e.ignore {
		h.size += int64(len(h.app.sharedMem.bufEntrySize) + e.size + len(h.app.sharedMem.bufEntryFlag))
	}
	return false, nil
}

func (h *sizeFoldHandler) Value() interface{} {
	return h.size
}

func (h *sizeFoldHandler) Values() []interface{} {
	return nil
}
