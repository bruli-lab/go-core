package cqs

func CommandHandlerMultiMiddleware(middlewares ...CommandHandlerMiddleware) CommandHandlerMiddleware {
	return func(h CommandHandler) CommandHandler {
		handler := h
		for _, m := range middlewares {
			handler = m(handler)
		}
		return CommandHandlerFunc(handler.Handle)
	}
}

func QueryHandlerMultiMiddleware(middlewares ...QueryHandlerMiddleware) QueryHandlerMiddleware {
	return func(h QueryHandler) QueryHandler {
		handler := h
		for _, m := range middlewares {
			handler = m(handler)
		}
		return QueryHandlerFunc(handler.Handle)
	}
}
