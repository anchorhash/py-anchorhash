from random import randrange
from typing import List, Dict, Tuple
from xxhash import xxh64_intdigest
from anchor.anchorhash import AnchorHash


class Anchor:
    """
    A consistent hash (key --> resource)
    """
    M: List[str]  # bijection resource <--> bucket
    M_inverse: Dict[str, int]

    def __init__(self, working_set: List[str], capacity: int = None, seed: int = randrange(1 << 32)) -> None:
        """Creates an AnchorHash Object

        Args:
            working_set:    list of working set resource
            capacity:       capacity of anchor set. default to 10% more than resource set
            seed:           optional random seed (unsigned 32 bit int to use with `xxhash`)

        Returns:
            Anchor wrapper object
        """
        self.seed = seed
        if len(working_set) < 1:
            raise ValueError("Must have at least one working resource")
        w = len(working_set)
        if capacity is None:
            a = int(1.1 * w)
        else:
            a = capacity
        self.M = working_set + ["" for _ in range(w, a)]
        self.M_inverse = dict([(resource, bucket) for (bucket, resource) in enumerate(self.M[:w])])
        self.anchor = AnchorHash(a=a, w=w)

    def get_resource(self, key: str) -> Tuple[str, int]:
        """Return resource for key
        Caches result

        Args:
            key

        Returns:
            name of resource, bucket
        """
        k = xxh64_intdigest(key, self.seed)
        b = self.anchor.get_bucket(k)
        s = self.M[b]
        return s, b

    def add_resource(self, s: str) -> int:
        """Add a new resource to the working set, return bucket

        Args:
            s:  name of resource to add. Must exist in Anchor set.

        Returns:
            bucket used for resource
        """
        if self.anchor.N == self.anchor.M:
            raise OverflowError("No room for more buckets")
        if s in self.M_inverse:
            raise KeyError("Resource {s} already exists".format(s=s))

        b = self.anchor.add_bucket()
        self.M[b] = s
        self.M_inverse[s] = b
        return b

    def remove_resource(self, s: str = None) -> Tuple[str, int]:
        """Remove a resource from the working set, return bucket
        Remove from cache any keys that map the resource

        Default:
            Remove resource with highest bucket

        Args:
            s:  name of resource to remove.
                If giver then must exist, default remove resource with highest bucket
                Must not remove last resource.

        Returns:
            bucket resource was at
        """
        if len(self.M) == 1:
            raise OverflowError("Cannot remove last resource")

        if s is not None:
            if s not in self.M_inverse:
                raise KeyError("resource {s} does not exist".format(s=s))
            b = self.M_inverse[s]
            self.anchor.remove_bucket(b)
        else:
            b = self.anchor.pop_bucket()
            s = self.M[b]

        self.M[b] = ""
        del self.M_inverse[s]

        return s, b

    def list_resources(self) -> List[str]:
        """
        List working resources
        """
        return list(self.M_inverse.keys())

    def size(self) -> int:
        """
        Size of working set
        """
        return self.anchor.N

    def capacity(self) -> int:
        """
        Capacity of anchor
        """
        return self.anchor.M
