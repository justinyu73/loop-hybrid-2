"""Native Windows file capability implementation; loaded without host API calls."""
from contextlib import contextmanager


class WindowsFileAdapter:
    """Lazy native file operations only; no process/provider backend."""

    def __init__(self, reason, *, unavailable, private_path):
        self._unavailable, self._private_path = unavailable, private_path
        import ctypes
        from ctypes import wintypes
        self.c, self.w, self.reason = ctypes, wintypes, reason
        c, w = ctypes, wintypes
        self.kernel = c.WinDLL("kernel32", use_last_error=True)
        self.adv = c.WinDLL("advapi32", use_last_error=True)
        p = c.c_void_p
        def bind(dll, name, result, args):
            fn = getattr(dll, name)
            fn.restype, fn.argtypes = result, args
            return fn
        class FileInfo(c.Structure):
            _fields_ = [("attributes", w.DWORD), ("creation", w.FILETIME),
                        ("access", w.FILETIME), ("write", w.FILETIME),
                        ("volume", w.DWORD), ("size_high", w.DWORD),
                        ("size_low", w.DWORD), ("links", w.DWORD),
                        ("index_high", w.DWORD), ("index_low", w.DWORD)]
        class AclInfo(c.Structure):
            _fields_ = [("count", w.DWORD), ("used", w.DWORD), ("free", w.DWORD)]
        class Mapping(c.Structure):
            _fields_ = [(name, w.DWORD) for name in ("read", "write", "execute", "all")]
        class SidAndAttributes(c.Structure):
            _fields_ = [("sid", p), ("attributes", w.DWORD)]
        class TokenOwner(c.Structure):
            _fields_ = [("owner", p)]
        self.FileInfo, self.AclInfo, self.Mapping = FileInfo, AclInfo, Mapping
        self.SidAndAttributes = SidAndAttributes
        self.TokenOwner = TokenOwner
        self.create = bind(self.kernel, "CreateFileW", w.HANDLE,
            [w.LPCWSTR,w.DWORD,w.DWORD,p,w.DWORD,w.DWORD,w.HANDLE])
        self.close = bind(self.kernel,"CloseHandle",w.BOOL,[w.HANDLE])
        self.free = bind(self.kernel,"LocalFree",p,[p])
        self.process = bind(self.kernel,"GetCurrentProcess",w.HANDLE,[])
        self.info = bind(self.kernel,"GetFileInformationByHandle",w.BOOL,[w.HANDLE,c.POINTER(FileInfo)])
        self.file_type = bind(self.kernel,"GetFileType",w.DWORD,[w.HANDLE])
        self.flush = bind(self.kernel,"FlushFileBuffers",w.BOOL,[w.HANDLE])
        self.get_security = bind(self.adv,"GetSecurityInfo",w.DWORD,
            [w.HANDLE,w.DWORD,w.DWORD]+[c.POINTER(p)]*5)
        self.set_security = bind(self.adv,"SetSecurityInfo",w.DWORD,
            [w.HANDLE,w.DWORD,w.DWORD,p,p,p,p])
        self.control = bind(self.adv,"GetSecurityDescriptorControl",w.BOOL,
            [p,c.POINTER(w.WORD),c.POINTER(w.DWORD)])
        self.acl_info = bind(self.adv,"GetAclInformation",w.BOOL,[p,p,w.DWORD,w.DWORD])
        self.get_ace = bind(self.adv,"GetAce",w.BOOL,[p,w.DWORD,c.POINTER(p)])
        self.equal_sid = bind(self.adv,"EqualSid",w.BOOL,[p,p])
        self.valid_sid = bind(self.adv,"IsValidSid",w.BOOL,[p])
        self.open_token = bind(self.adv,"OpenProcessToken",w.BOOL,
            [w.HANDLE,w.DWORD,c.POINTER(w.HANDLE)])
        self.token_info = bind(self.adv,"GetTokenInformation",w.BOOL,
            [w.HANDLE,w.DWORD,p,w.DWORD,c.POINTER(w.DWORD)])
        self.duplicate = bind(self.adv,"DuplicateToken",w.BOOL,
            [w.HANDLE,c.c_int,c.POINTER(w.HANDLE)])
        self.access = bind(self.adv,"AccessCheck",w.BOOL,
            [p,w.HANDLE,w.DWORD,c.POINTER(Mapping),p,c.POINTER(w.DWORD),
             c.POINTER(w.DWORD),c.POINTER(w.BOOL)])
        self.sid_length = bind(self.adv,"GetLengthSid",w.DWORD,[p])
        self.init_acl = bind(self.adv,"InitializeAcl",w.BOOL,[p,w.DWORD,w.DWORD])
        self.add_ace = bind(self.adv,"AddAccessAllowedAceEx",w.BOOL,
            [p,w.DWORD,w.DWORD,w.DWORD,p])

    def fail(self, stage, error=None):
        if error is None:
            error = self.c.get_last_error()
        raise self._unavailable(
            f"{self.reason}:{stage}:winerror={int(error)}")

    def check(self, success, stage):
        if not success:
            self.fail(stage)

    def guard(self, path):
        try:
            return self._private_path(path)
        except self._unavailable as exc:
            raise self._unavailable(f"{self.reason}:unsafe_path") from exc

    def identity(self, handle, *, directory=False):
        information = self.FileInfo()
        self.check(self.info(handle,self.c.byref(information)), "file_identity")
        if (self.file_type(handle) != 1
                or bool(information.attributes & 0x10) != directory
                or information.attributes & 0x400):
            self.fail("file_type", 0)
        return (information.volume, information.index_high, information.index_low)

    def open(self, path, access, *, directory=False):
        path = self.guard(path)
        handle = self.create(str(path),access,7,None,3,0x02000000|0x00200000,None)
        if handle == self.c.c_void_p(-1).value or handle is None:
            self.fail("open")
        try:
            self.identity(handle,directory=directory)
            self.guard(path)
        except BaseException:
            self.close(handle)
            raise
        return handle

    @contextmanager
    def descriptor(self, handle):
        c = self.c
        owner,group,dacl,sacl,sd = (c.c_void_p() for _ in range(5))
        code = self.get_security(handle,1,0x7,c.byref(owner),c.byref(group),
                                 c.byref(dacl),c.byref(sacl),c.byref(sd))
        if code:
            self.fail("get_security",code)
        try:
            if not owner.value or not group.value or not dacl.value:
                self.fail("missing_descriptor",0)
            if not self.valid_sid(owner) or not self.valid_sid(group):
                self.fail("invalid_descriptor_sid",0)
            yield owner,group,dacl,sd
        finally:
            self.free(sd)

    @contextmanager
    def token(self):
        c,w = self.c,self.w
        token = w.HANDLE()
        self.check(self.open_token(self.process(),0xA,c.byref(token)), "open_token")
        try:
            size = w.DWORD()
            self.token_info(token,1,None,0,c.byref(size))
            if size.value < c.sizeof(self.SidAndAttributes):
                self.fail("token_size")
            user_buffer = c.create_string_buffer(size.value)
            self.check(self.token_info(token,1,user_buffer,len(user_buffer),c.byref(size)), "token_user")
            user_sid = c.cast(user_buffer,c.POINTER(self.SidAndAttributes)).contents.sid
            if not user_sid or not self.valid_sid(user_sid):
                self.fail("token_sid",0)
            owner_size = w.DWORD()
            self.token_info(token,4,None,0,c.byref(owner_size))
            if owner_size.value < c.sizeof(self.TokenOwner):
                self.fail("token_owner_size")
            owner_buffer = c.create_string_buffer(owner_size.value)
            self.check(self.token_info(token,4,owner_buffer,len(owner_buffer),c.byref(owner_size)),
                       "token_owner")
            owner_sid = c.cast(owner_buffer,c.POINTER(self.TokenOwner)).contents.owner
            if not owner_sid or not self.valid_sid(owner_sid):
                self.fail("token_owner_sid",0)
            # Both SID pointers borrow their buffers, retained across the yield.
            # TokenOwner is OS-configured ownership; only TokenUser receives the ACE.
            yield token,user_sid,owner_sid
        finally:
            self.close(token)

    def require_owner(self, handle, owner_sid):
        with self.descriptor(handle) as (owner,group,dacl,sd):
            if not self.equal_sid(owner,owner_sid):
                self.fail("owner_mismatch",0)

    def verify(self, handle):
        import struct
        c,w = self.c,self.w
        with self.token() as (token,user_sid,owner_sid), self.descriptor(handle) as (owner,group,dacl,sd):
            if not self.equal_sid(owner,owner_sid):
                self.fail("owner_mismatch",0)
            control,revision = w.WORD(),w.DWORD()
            self.check(self.control(sd,c.byref(control),c.byref(revision)), "descriptor_control")
            if not control.value & 0x1000:
                self.fail("dacl_not_protected",0)
            info = self.AclInfo()
            self.check(self.acl_info(dacl,c.byref(info),c.sizeof(info),2), "acl_info")
            if info.count != 1:
                self.fail("dacl_not_private",0)
            ace = c.c_void_p()
            self.check(self.get_ace(dacl,0,c.byref(ace)), "ace")
            kind,flags,size,mask = struct.unpack("<BBHI",c.string_at(ace,8))
            if kind != 0 or flags != 0 or size < 12 or mask != 0x12019F:
                self.fail("ace_not_private",0)
            ace_sid = c.c_void_p(ace.value+8)
            if not self.valid_sid(ace_sid) or not self.equal_sid(ace_sid,user_sid):
                self.fail("ace_principal",0)
            duplicate = w.HANDLE()
            self.check(self.duplicate(token,2,c.byref(duplicate)), "duplicate_token")
            try:
                mapping = self.Mapping(0x120089,0x120116,0x1200A0,0x1F01FF)
                privileges = c.create_string_buffer(4096)
                size,granted,status = w.DWORD(len(privileges)),w.DWORD(),w.BOOL()
                self.check(self.access(sd,duplicate,0x12019F,c.byref(mapping),privileges,
                           c.byref(size),c.byref(granted),c.byref(status)), "access_check")
                if not status.value or granted.value & 0x12019F != 0x12019F:
                    self.fail("effective_access",0)
            finally:
                self.close(duplicate)

    def protect(self, handle):
        c = self.c
        with self.token() as (token,user_sid,owner_sid):
            self.require_owner(handle,owner_sid)
            # ACL header (8) + ACCESS_ALLOWED_ACE header/mask (8) + SID.
            size = 16 + self.sid_length(user_sid)
            acl = c.create_string_buffer(size)
            self.check(self.init_acl(acl,size,2), "initialize_acl")
            self.check(self.add_ace(acl,2,0,0x12019F,user_sid), "add_private_ace")
            code = self.set_security(handle,1,0x80000004,None,None,acl,None)
            if code:
                self.fail("set_private_dacl",code)
        self.verify(handle)

    @staticmethod
    def borrowed_handle(fd):
        import msvcrt
        return msvcrt.get_osfhandle(fd)
