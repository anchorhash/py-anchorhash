from xxhash import xxh64_intdigest


class AnchorHash:
    """
    The brain of AnchorHash, implements a consistent hash (int key --> bucket index)
    """

    def __init__(self, a: int, w: int) -> None:
        """Creates an AnchorHash Object

        Assumes int buckets and int keys

        Args:
            a:      size of anchor set
            w:      size of working set

        Returns:
            AnchorHash object
        """
        self.M = a
        self.N = a

        # anchor Set
        self.A = [0 for _ in range(a)]

        # working set
        self.W = [x for x in range(a)]

        # last bucket location
        self.L = [x for x in range(a)]

        # successor
        self.K = [x for x in range(a)]

        # removed buckets stack
        self.R = []

        # for i in reversed(range(w, a)):
        #     self.remove_bucket(i)
        for i in range(w, a):
            self.pop_bucket()

    def get_bucket(self, k: int) -> int:
        """Calculates bucket for key

        :param k: key, assumed to be uniform (already hashed)
        :return: assigned bucket
        """
        # uncomment next line if key not already hashed
        # k = xxh64_intdigest(bin(k), k)
        b = k % self.M
        while self.A[b] > 0:  # b is removed
            # next line is like random(seed=k,b)
            # could instead use: k = int(0xFFFFFFFFFFFFFFFF & (k * 2862933555777941757 + 1))
            k = xxh64_intdigest(bin(k)+bin(b), k)
            h = k % self.A[b]
            while self.A[h] >= self.A[b]:  # b removed prior to h
                h = self.K[h]
            b = h
        return b

    def add_bucket(self) -> int:
        """Add a new bucket. The algorithm chooses the new bucket number

        :return: new bucket
        """
        b = self.R.pop()
        self.A[b] = 0
        self.L[self.W[self.N]] = self.N
        self.W[self.L[b]] = b
        self.K[b] = b
        self.N += 1
        return b

    def remove_bucket(self, b: int) -> None:
        """Remove a working bucket

        :param b: bucket to remove
        """
        self.N -= 1
        self.A[b] = self.N
        self.W[self.L[b]] = self.W[self.N]
        self.L[self.W[self.N]] = self.L[b]
        self.K[b] = self.W[self.N]
        self.R.append(b)

    def pop_bucket(self):
        """Remove bucket with highest location

        :return: removed bucket
        """
        self.N -= 1
        b = self.W[self.N]
        self.A[b] = self.N
        self.R.append(b)
        return b
