# Copyright (c) Genome Research Ltd 2012
# Copyright (c) Universite Laval 2018
# Author
# Guy Coates <gmpc@sanger.ac.uk>
# Simon Guilbault <simon.guilbault@calculquebec.ca>
# This program is released under the GNU Public License V2 (GPLv2)
# Based on https://github.com/wtsi-ssg/pcp/blob/master/pcplib/lustreapi.py

"""
Python bindings to minimal subset of lustre api.
This module requires a dynamically linked version of the lustre
client library (liblustreapi.so).

Older version of the lustre client only ships a static library
(liblustreapi.a).
setup.py should have generated a dynamic version during installation.

You can generate the dynamic library by hand by doing the following:

ar -x liblustreapi.a
gcc -shared -o liblustreapi.so *.o

"""
import ctypes
import ctypes.util
import os
import select


import pkg_resources
try:
    __version__ = pkg_resources.require("pcp")[0].version
except pkg_resources.DistributionNotFound:
    __version__ = "UNRELEASED"

LUSTREMAGIC = 0xbd00bd0
HSM_FLAGS = [
    ('NONE', "0x00000000"),
    ('EXISTS', "0x00000001"),
    ('DIRTY', "0x00000002"),
    ('RELEASED', "0x00000004"),
    ('ARCHIVED', "0x00000008"),
    ('NORELEASE', "0x00000010"),
    ('NOARCHIVE', "0x00000020"),
    ('LOST', "0x00000040"),
]

liblocation = ctypes.util.find_library("lustreapi")
# See if liblustreapi.so is in the same directory as the module
if not liblocation:
    modlocation, module = os.path.split(__file__)
    liblocation = os.path.join(modlocation, "liblustreapi.so")

lustre = ctypes.CDLL(liblocation, use_errno=True)


# ctype boilerplate for C data structures and functions
class lov_user_ost_data_v1(ctypes.Structure):
    _fields_ = [
        ("l_object_id", ctypes.c_ulonglong),
        ("l_object_gr", ctypes.c_ulonglong),
        ("l_ost_gen", ctypes.c_uint),
        ("l_ost_idx", ctypes.c_uint)
        ]


class lov_user_md_v1(ctypes.Structure):
    _fields_ = [
        ("lmm_magic", ctypes.c_uint),
        ("lmm_pattern", ctypes.c_uint),
        ("lmm_object_id", ctypes.c_ulonglong),
        ("lmm_object_gr", ctypes.c_ulonglong),
        ("lmm_stripe_size", ctypes.c_uint),
        ("lmm_stripe_count",  ctypes.c_short),
        ("lmm_stripe_offset", ctypes.c_short),
        ("lmm_objects", lov_user_ost_data_v1 * 2000),
        ]


class hsm_user_state(ctypes.Structure):
    _fields_ = [
        ("hus_states", ctypes.c_uint),
        ("hus_archive_id", ctypes.c_uint),
        ]


class lu_fid(ctypes.Structure):
    _fields_ = [
        ("f_seq", ctypes.c_ulonglong),
        ("f_oid", ctypes.c_uint),
        ("f_ver", ctypes.c_uint),
        ]


class Fid:
    def __init__(self, seq, oid, ver):
        self.seq = int(seq)
        self.oid = int(oid)
        self.ver = int(ver)

    def __str__(self):
        string = "[0x%x:0x%x:0x%x]" \
                 % (self.seq, self.oid, self.ver)
        return(string)


class HSM_state:
    """
    """
    def __init__(self, hus):
        self.archive_id = int(hus.hus_archive_id)
        self.states = []
        state = int(hus.hus_states)
        for flag in HSM_FLAGS:
            if state & int(flag[1], 16):
                self.states.append(flag[0])

    def __str__(self):
        if len(self.states) > 1:
            string = "Archive id:%i" % self.archive_id
            string += "\nStates: "
            states = " ".join(self.states)
            return string + states
        else:
            return "No HSM state"


def hsm_state_from_flags(flags):
    state = 0
    for flag_def in HSM_FLAGS:
        for flag in flags:
            if flag_def[0] == flag:
                state += int(flag_def[1], 16)
    return state


class hsm_copytool_private(ctypes.Structure):
    pass


lov_user_md_v1_p = ctypes.POINTER(lov_user_md_v1)
hsm_user_state_p = ctypes.POINTER(hsm_user_state)

lustre.llapi_file_get_stripe.argtypes = [ctypes.c_char_p, lov_user_md_v1_p]
lustre.llapi_file_open.argtypes = [ctypes.c_char_p, ctypes.c_int,
                                   ctypes.c_int, ctypes.c_ulong, ctypes.c_int,
                                   ctypes.c_int, ctypes.c_int]
lustre.llapi_hsm_state_get.argtypes = [ctypes.c_char_p, hsm_user_state_p]
lustre.llapi_hsm_state_set.argtypes = [ctypes.c_char_p, ctypes.c_uint,
                                       ctypes.c_uint, ctypes.c_uint]


class stripeObj:
    """
    lustre stripe object.

    This object contains details of the striping of a lustre file.

    Attributes:
      lovdata:  lov_user_md_v1 structure as returned by the lustre C API.
      stripecount: Stripe count.
      stripesize:  Stripe size (bytes).
      stripeoffset: Stripe offset.
      ostobjects[]: List of lov_user_ost_data_v1 structures as returned by the
      C API.
    """
    def __init__(self):
        self.lovdata = lov_user_md_v1()
        self.stripecount = -1
        self.stripesize = 0
        self.stripeoffset = -1
        self.ostobjects = []

    def __str__(self):
        string = "Stripe Count: %i Stripe Size: %i Stripe Offset: %i\n" \
                 % (self.stripecount, self.stripesize, self.stripeoffset)
        for ost in self.ostobjects:
            string += ("Objidx:\t %i \tObjid:\t %i\n" % (ost.l_ost_idx,
                                                         ost.l_object_id))
        return(string)

    def isstriped(self):
        if self.stripecount > 1 or self.stripecount == -1:
            return(True)
        else:
            return(False)


def getstripe(filename):
    """Returns a stripeObj containing the stipe information of filename.

    Arguments:
      filename: The name of the file to query.

    Returns:
      A stripeObj containing the stripe information.
    """
    stripeobj = stripeObj()
    lovdata = lov_user_md_v1()
    stripeobj.lovdata = lovdata
    err = lustre.llapi_file_get_stripe(filename, ctypes.byref(lovdata))

    # err 61 is due to  LU-541 (see below)
    if err < 0 and err != -61:
        err = 0 - err
        raise IOError(err, os.strerror(err))

    # workaround for Whamcloud LU-541
    # use the filesystem defaults if no properties set
    if err == -61:
        stripeobj.stripecount = 0
        stripeobj.stripesize = 0
        stripeobj.stripeoffset = -1

    else:
        for i in range(0, lovdata.lmm_stripe_count):
            stripeobj.ostobjects.append(lovdata.lmm_objects[i])

        stripeobj.stripecount = lovdata.lmm_stripe_count
        stripeobj.stripesize = lovdata.lmm_stripe_size
        # lmm_stripe_offset seems to be reported as 0, which is wrong
        if len(stripeobj.ostobjects) > 0:
            stripeobj.stripeoffset = stripeobj.ostobjects[0].l_ost_idx
        else:
            stripeobj.stripeoffset = -1
    return(stripeobj)


def setstripe(filename, stripeobj=None, stripesize=0, stripeoffset=-1,
              stripecount=1):
    """Sets the striping on an existing directory, or create a new empty file
    with the specified striping. Stripe parameters can be set explicity, or
    you can pass in an existing stripeobj to copy the attributes from an
    existing file.

    Note you can set the striping on an existing directory, but you cannot set
    the striping on an existing file.

    Arguments:
      stripeobj: copy the parameters from stripeobj.
      stripesize: size of stripe in bytes
      stripeoffset: stripe offset
      stripecount: stripe count

    Examples:
      #Set the filesystem defaults
      setstripe("/lustre/testfile")

      # Stripe across all OSTs.
      setstripe("/lustre/testfile", stripecount=-1)

      #copy the attributes from foo
      stripeobj = getstripe("/lustre/foo")
      setstripe("/lustre/testfile", stripeobj)

    """
    flags = os.O_CREAT
    mode = '0700'
    # only stripe_pattern 0 is supported by lustre.
    stripe_pattern = 0

    if stripeobj:
        stripesize = stripeobj.stripesize
        stripeoffset = stripeobj.stripeoffset
        stripecount = stripeobj.stripecount

    # Capture the lustre error messages, These get printed to stderr via
    # liblusteapi, and so we need to intercept them.

    message = captureStderr()

    fd = lustre.llapi_file_open(filename, flags, mode, stripesize,
                                stripeoffset, stripecount, stripe_pattern)
    message.readData()
    message.stopCapture()

    if fd < 0:
        err = 0 - fd
        raise IOError(err, os.strerror(err))
    else:
        os.close(fd)
        return(0)


def path2fid(filename):
    lufid = lu_fid()
    err = lustre.llapi_path2fid(
        filename.encode('utf8'),
        ctypes.byref(lufid))
    if err < 0:
        err = 0 - err
        raise IOError(err, os.strerror(err))
    fid = Fid(lufid.f_seq, lufid.f_oid, lufid.f_ver)
    return fid


def get_hsm_state(filename):
    hus = hsm_user_state()
    err = lustre.llapi_hsm_state_get(
        filename.encode('utf8'),
        ctypes.byref(hus))
    if err < 0:
        err = 0 - err
        raise IOError(err, os.strerror(err))
    return HSM_state(hus)


def set_hsm_state(filename, setmask, clearmask, archive_id):
    print(filename, hsm_state_from_flags(setmask), hsm_state_from_flags(clearmask), archive_id)
    err = lustre.llapi_hsm_state_set(
        filename.encode('utf8'),
        hsm_state_from_flags(setmask),
        hsm_state_from_flags(clearmask),
        archive_id)
    if err < 0:
        err = 0 - err
        raise IOError(err, os.strerror(err))


class hsm_agent():
    """Not currently working"""
    def __init__(self):
        self.hsm_copytool_private = hsm_copytool_private()

    def __del__(self):
        self.hsm_copytool_unregister()

    def hsm_copytool_register(self, mnt, archives=[1], rfd_flags=0):
        archives_arr = (ctypes.c_int * len(archives))(*archives)
        archives_p = ctypes.POINTER(ctypes.c_int)
        lustre.llapi_hsm_copytool_register.argtypes = [
            ctypes.POINTER(hsm_copytool_private), ctypes.c_char_p,
            ctypes.c_int, ctypes.POINTER(ctypes.c_int), ctypes.c_int]
        err = lustre.llapi_hsm_copytool_register(
            ctypes.byref(self.hsm_copytool_private),
            mnt,
            len(archives),
            archives_arr,
            rfd_flags)
        if err < 0:
            err = 0 - err
            raise IOError(err, os.strerror(err))

    def hsm_copytool_unregister(self):
        lustre.llapi_hsm_copytool_unregister.argtypes = [
            ctypes.POINTER(hsm_copytool_private)]
        err = lustre.llapi_hsm_copytool_unregister(
            ctypes.byref(self.hsm_copytool_private))
        if err < 0:
            err = 0 - err
            raise IOError(err, os.strerror(err))


class captureStderr():
    """This class intercepts stderr and stores any output"""
    def __init__(self):
        self.pipeout, self.pipein = os.pipe()
        self.oldstderr = os.dup(2)
        os.dup2(self.pipein, 2)
        self.contents = ""

    def __str__(self):
        return (self.contents)

    def readData(self):
        """Read data from stderr until there is no more."""
        while self.checkData():
            self.contents += os.read(self.pipeout, 1024)

    def checkData(self):
        """Check to see if there is any data to be read."""
        r, _, _ = select.select([self.pipeout], [], [], 0)
        return bool(r)

    def stopCapture(self):
        """Restore the original stderr"""
        os.dup2(self.oldstderr, 2)
        os.close(self.pipeout)
        os.close(self.pipein)
