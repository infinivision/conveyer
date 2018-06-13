FROM busybox

ADD conveyer /bin/

ENTRYPOINT [ "/bin/conveyer" ]
