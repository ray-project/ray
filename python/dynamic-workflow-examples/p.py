def continuation_f659cb21_abf9_11ec_872d_0698c5eeed07(r, w):
    diff_id = "f659cb21_abf9_11ec_872d_0698c5eeed07"
    print("INSIDE CONTINUATION", w)
    r.resume_ret = w
    assert r.task != None
    c = r.run_once(r.task)
    print("CONTINUATION:c", c)
    if r.finished != True:
        conn = gen_continuation()
        return ray.workflow.step(conn).options(_with_resumable=True).step(r, c)
    else:
        return c
