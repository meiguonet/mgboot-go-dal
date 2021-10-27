package dbx

type DbException struct {
	errorTips string
}

func NewDbException(errorTips string) DbException {
	return DbException{errorTips: errorTips}
}

func (ex DbException) Error() string {
	return ex.errorTips
}

func toDbException(err error) DbException {
	if ex, ok := err.(DbException); ok {
		return ex
	}

	return DbException{errorTips: err.Error()}
}
