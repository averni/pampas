Pampas:

Idee:

Design:

Utilizzo:
- Testing in locale:
    import os, errno
    def mkdir_p(path):
        try:
            os.makedirs(path)
        except OSError as exc: # Python >2.5
            if exc.errno == errno.EEXIST:
                pass
            else: raise

    messagequeue = '/queue/social'
    basepath = '/home/antonio/code/trunk/commons/python/pampas/test/filemq'
    mkdir_p(basepath)
    mf = TestFactory(basepath)
    mf.setMessageQueue(messagequeue)

- Producer:
    prod = mf.createProducer()
    prod.sendMessage({"testo":"foo"})

- Consumer:
    def f(h,m):
        logger.info("FunzioneF::Messaggio: %s - %s" % (str(h),str(m))
    consumer = mf.createConsumer(f)
    with consumer:
        consumer.run()

Esempi:


