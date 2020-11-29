from random import Random
from anchor.wrapper import Anchor
from math import ceil
from itertools import product


def gen_seeds(seed=None):
    r = Random(seed)
    return (r.randrange(1 << 32) for _ in range(4))


def resources(capacity, factor, seed):
    r = Random(seed)
    a = capacity
    w = ceil(a / factor)

    anchor_set = ["resource_{x}".format(x=x) for x in range(a)]
    removed_set = r.sample(anchor_set, a - w)
    working_set = list(set(anchor_set) - set(removed_set))

    return w, a, anchor_set, removed_set, working_set


def keys(nkeys=1000, seed=None):
    r = Random(seed)
    return ["key_{x}".format(x=x) for x in r.sample(range(1 << 32), nkeys)]


def balance(capacity: int, factor: float = 1.1, rnd_removes: bool = True,
            nkeys: int = 1000, seed: int = None) -> None:
    print('-' * 60)
    key_seed, anchor_seed, remove_seed, move_seed = gen_seeds(seed)

    w, a, anchor_set, removed_set, working_set = resources(capacity, factor, remove_seed)

    if rnd_removes:
        anchor_hash = Anchor(anchor_set, capacity=a, seed=anchor_seed)
        for q in removed_set:
            anchor_hash.remove_resource(q)
    else:
        anchor_hash = Anchor(working_set, capacity=a, seed=anchor_seed)

    hist = [0] * a

    print("Balance: ", dict(a=a, w=w, rnd_removes=rnd_removes, nkeys=nkeys, seed=seed))

    for key in keys(nkeys, key_seed):
        s, b = anchor_hash.get_resource(key)
        if s not in working_set:
            print("ERROR bad bucket: ", dict(s=s, b=b))
        hist[b] += 1

    total = sum(hist)
    if total != nkeys:
        print("ERROR: not all keys recorded")

    high = max(hist)
    low = min(x for x in hist if x > 0)
    avg = int(nkeys / w)

    print(dict(high=high, low=low, avg=avg, total=total, load_pct=float(100 * high / avg) - 100))
    print('=' * 60)


def consistency(capacity: int, factor: float = 1.1, nmoves: int = None,
                nkeys: int = 1000, seed: int = None) -> None:
    print('-' * 60)

    key_seed, anchor_seed, remove_seed, move_seed = gen_seeds(seed)

    w, a, anchor_set, removed_set, working_set = resources(capacity, factor, remove_seed)

    if nmoves is None:
        nmoves = int(w / 2)
    rnd = Random(move_seed)

    current_hash = Anchor(working_set, capacity=a, seed=anchor_seed)
    previous_hash = Anchor(working_set, capacity=a, seed=anchor_seed)

    print("Consistency: ", dict(a=a, w=w, nmoves=nmoves, nkeys=nkeys, seed=seed))

    moves = ["add", "remove"]

    for i in range(nmoves):
        move = rnd.choice(moves)

        if move == "add":
            if len(removed_set) == 0:
                continue
            s = rnd.choice(removed_set)
            removed_set.remove(s)
            working_set.append(s)
            current_hash.add_resource(s)
        else:  # "remove"
            if len(working_set) == 1:
                continue
            s = rnd.choice(working_set)
            removed_set.append(s)
            working_set.remove(s)
            current_hash.remove_resource(s)

        for key in keys(nkeys, key_seed):
            curr_s, curr_b = current_hash.get_resource(key)
            prev_s, prev_b = previous_hash.get_resource(key)
            if curr_s != prev_s and (curr_s, prev_s)[moves.index(move)] != s:
                print("Error after move: ", dict(move=move, s=s, key=key,
                                                 curr_s=curr_s, curr_b=curr_b, prev_s=prev_s, prev_b=prev_b))
                print('=' * 60)
                return

        if move == "add":
            previous_hash.add_resource(s)
        else:  # "remove"
            previous_hash.remove_resource(s)
    print('=' * 60)


def rate_setup(capacity, factor=1.1, rnd_removes=True, nkeys=1000, seed=None):
    key_seed, anchor_seed, remove_seed, move_seed = gen_seeds(seed)

    w, a, anchor_set, removed_set, working_set = resources(capacity, factor, remove_seed)

    if rnd_removes:
        anchor_hash = Anchor(anchor_set, capacity=a, seed=anchor_seed)
        for q in removed_set:
            anchor_hash.remove_resource(q)
    else:
        anchor_hash = Anchor(working_set, capacity=a, seed=anchor_seed)

    key_gen = keys(nkeys, key_seed)

    return anchor_hash, key_gen


def rate(capacity: int, factor: float = 1.1, rnd_removes: bool = True,
         nkeys: int = 1000, repeat: int = 5, seed: int = None) -> None:
    import timeit
    setup = '''
from __main__ import rate_setup
anchor_hash, keys = rate_setup({capacity}, {factor}, {rnd_removes}, {nkeys}, {seed})
    '''.format(capacity=capacity, factor=factor, rnd_removes=rnd_removes, nkeys=nkeys, seed=seed)
    code = '''
for k in keys:
    anchor_hash.get_resource(k)
    '''

    print('-' * 60)
    print("Rate: ", dict(capacity=capacity, factor=factor, rnd_removes=rnd_removes, nkeys=nkeys, repeat=repeat, seed=seed))

    max_rate = 0
    times = timeit.repeat(setup=setup, stmt=code, number=repeat, repeat=5)
    for time in times:
        avg_micro = round((time * 1000 * 1000) / (repeat * nkeys), 2)
        msec = round(time * 1000, 2)
        per_sec = round((nkeys * repeat) / (time * 1000), 2)
        print(dict(msec=msec, avg_microsec_per_key=avg_micro, k_keys_per_sec=per_sec))
        max_rate = max(max_rate, per_sec)
    print('max rate (keys/sec) is: {rate:,}'.format(rate=ceil(1000*max_rate)))
    print('=' * 60)


def example(seed=None):
    if seed is None:
        seed = Random(seed).randrange(1 << 32)
    print('-' * 60)
    print("EXAMPLE", dict(seed=seed))
    workers = ['a', 'b', 'C', 'd', 'e', 'f', 'g']
    anchor = Anchor(workers, 10, seed=seed)
    keys = "anchor hash is the greatest thing since instant coffee"
    print("keys: {keys}".format(keys="|".join(keys)))
    print("hash: {res}, workers: {workers}, size: {size}, capacity: {capacity}".format(
        res="|".join([anchor.get_resource(key)[0] for key in keys]), workers=anchor.list_resources(),
        size=anchor.size(), capacity=anchor.capacity()))
    anchor.add_resource('H')
    print("hash: {res}, workers: {workers}, size: {size}, capacity: {capacity}".format(
        res="|".join([anchor.get_resource(key)[0] for key in keys]), workers=anchor.list_resources(),
        size=anchor.size(), capacity=anchor.capacity()))
    anchor.remove_resource('C')
    print("hash: {res}, workers: {workers}, size: {size}, capacity: {capacity}".format(
        res="|".join([anchor.get_resource(key)[0] for key in keys]), workers=anchor.list_resources(),
        size=anchor.size(), capacity=anchor.capacity()))
    print('-' * 60)


def main():
    seed = 1984

    print("TESTING ", dict(seed=seed))
    example(seed)
    for capacity, factor, nkeys in product([100, 200, 500, 1000], [1.1, 2, 10, 50], [1000, 1000000]):
        balance(capacity=capacity, factor=factor, rnd_removes=True, nkeys=nkeys, seed=seed)
        consistency(capacity=capacity, factor=factor, nmoves=100, nkeys=ceil(nkeys/100), seed=seed)
        rate(capacity=capacity, factor=factor, rnd_removes=True, nkeys=nkeys, repeat=10, seed=seed)


if __name__ == "__main__":
    main()
