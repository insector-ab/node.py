# -*- coding: utf-8 -*-
import webob


class Request(webob.Request):

    def __init__(self, environ, *args, **kws):
        super(Request, self).__init__(environ, *args, **kws)

    @property
    def session(self):
        return self.environ['node.session']