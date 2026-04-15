class Runner(object):
    @staticmethod
    def runner(file_object):
        # Step 1: read ALL bytes from S3
        raw_bytes = file_object.read()

        # Step 2: decode safely (Windows CSV source)
        text = raw_bytes.decode("windows-1252", errors="ignore")

        # Step 3: yield line-by-line
        for line in text.splitlines(keepends=True):
            yield line.replace("\0", "")