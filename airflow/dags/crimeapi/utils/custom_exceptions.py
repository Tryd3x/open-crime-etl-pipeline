class APIPageFetchError(Exception):

    def __init__(self, message: str, pagenum: int, date: str):
        super().__init__(message)
        self.pagenum = pagenum
        self.date = date
        