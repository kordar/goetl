package managed_source

func (s *ManagedSource) Use(ext ManagedSourceExtension) {
	if ext == nil {
		return
	}
	s.mu.Lock()
	s.exts = append(s.exts, ext)
	started := s.started
	ctx := s.ctx
	s.mu.Unlock()

	if started && ctx != nil {
		go func() { _ = ext.Start(ctx, s) }()
	}
}
