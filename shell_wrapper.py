# /usr/bin/python
# coding: utf-8

from __future__ import print_function, division, unicode_literals

import os
import subprocess


class ShellWrapper(object):
    def __init__(self, log_key):
        self.LOG_KEY = log_key

    def shell(self, cmd):
        """execute shell commands
        """
        success = False
        try:
            print(cmd)
            success = (os.system(cmd) == 0)
        except Exception as ex:
            print("!!!!!! {} has error: {}".format(self.LOG_KEY, str(ex)))
            success = False
        return success

    def shell_with_return(self, cmd):
        print(cmd)
        # must set shell=True
        # and check_output return Bytes, hence must decode
        return subprocess.check_output(cmd, shell=True).decode('utf-8')


class FileProc(ShellWrapper):
    """provide file-related operations
    """

    def __init__(self):
        ShellWrapper.__init__(self, "file_proc")

    def rm_file(self, file_path):
        """delete a file
        """
        if not os.path.exists(file_path):
            return True
        cmd = 'rm -f %s' % file_path
        return self.shell(cmd)

    def rm_folder(self, folder_path):
        """delete a folder
        """
        if not os.path.exists(folder_path):
            return True
        cmd = 'rm -rf %s' % folder_path
        return self.shell(cmd)

    def gunzip(self, gz_file):
        """decompress gz file
        """
        if not os.path.exists(gz_file):
            return False
        cmd = 'gunzip %s' % gz_file
        return self.shell(cmd)

    def md5sum(self, input_file, output_md5_file):
        """generate md5 file
        """
        if not os.path.exists(input_file):
            return False
        cmd = 'md5sum %s > %s' % (input_file, output_md5_file)
        return self.shell(cmd)

    def mv_file(self, src_file, target_file):
        """move file
        """
        if not os.path.exists(src_file):
            return False
        cmd = 'mv -f %s %s' % (src_file, target_file)
        return self.shell(cmd)

    def mv_folder(self, src_folder, target_folder):
        """move a folder
        """
        if not self.rm_folder(target_folder):
            return False
        return self.mv_file(src_folder, target_folder)

    def clear_folder(self, src_folder):
        """clear all under a folder
        """
        if not os.path.exists(src_folder):
            return True
        src_folder = src_folder.rstrip('/')
        cmd = 'rm -rf %s/*' % src_folder
        return self.shell(cmd)

    def cp_file(self, src_file, target_file):
        """copy file
        """
        if not os.path.exists(src_file):
            return False
        cmd = 'cp -f %s %s' % (src_file, target_file)
        return self.shell(cmd)

    def mkdir(self, target_folder):
        """make a new folder
        """
        if os.path.exists(target_folder):
            return True
        cmd = 'mkdir -p %s' % (target_folder)
        return self.shell(cmd)

    def mail(self, receivers, subject, content):
        """email
        """
        if len(receivers) == 0:
            return False
        cmd = 'echo -e \'%s\' | mail -s \"%s\" %s' % (content, subject, ','.join(receivers))
        return self.shell(cmd)

    def wget(self, link, local_file):
        """download files
        """
        cmd = 'wget %s -O %s' % (link, local_file)
        return self.shell(cmd)

    def exists(self, path):
        """whether a path exists
        """
        return os.path.exists(path)

    def cmp_md5(self, first_md5_file, second_md5_file):
        """compare md5 string
        """
        first_md5_str = open(first_md5_file, 'r').readline().split(' ')[0]
        second_md5_str = open(second_md5_file, 'r').readline().split(' ')[0]
        return first_md5_str == second_md5_str

    def tar_gz_with_C(self, parent_folder, data_folder, output_targz):
        if self.exists(output_targz):
            self.rm_file(output_targz)
        cmd = "tar czvf %s -C %s %s" % (output_targz, parent_folder.rstrip("/"), data_folder)
        return self.shell(cmd)

    def tar_gz(self, src_file_folder, output_targz):
        if self.exists(output_targz):
            self.rm_file(output_targz)
        cmd = 'tar czvf %s %s/*' % (output_targz, src_file_folder.rstrip("/"))
        return self.shell(cmd)

    def tar_gz_only_files(self, src_file_folder, output_targz):
        """compress files into .tar.gz (note: path excluded)
        """
        if not self.exists(src_file_folder):
            return False
        cmd = 'tar zcf %s -C %s .' % (output_targz, src_file_folder)
        return self.shell(cmd)

    def file_size(self, filename):
        """get the file size in bytes
        """
        return os.path.getsize(filename) if self.exists(filename) else 0


class HadoopProc(ShellWrapper):
    """encapsulate hadoop client operations
    """

    def __init__(self, hadoop_dir=''):
        """hadoop_dir='' will be ignored when constructing the path
        """
        ShellWrapper.__init__(self, "hadoop_proc")
        self._hadoop_dir = hadoop_dir

    @property
    def hdfs_cmd(self):
        # if hadoop_dir is '', it will be ignored automatically
        return os.path.join(self._hadoop_dir, 'hdfs')

    def exists(self, hdfs_path):
        """whether the file exists on hdfs
        """
        success = False
        try:
            cmd = 'hadoop fs -test -e %s' % (hdfs_path)
            success = self.shell(cmd)
        except Exception as ex:
            print("!!!!!! {} has error: {}".format(self.LOG_KEY, str(ex)))
            success = False
        return success

    def touchz(self, hdfs_file):
        """make an empty file on hdfs
        """
        success = False
        try:
            cmd = 'hadoop fs -touchz %s' % (hdfs_file)
            success = self.shell(cmd)
        except Exception as ex:
            print("!!!!!! {} has error: {}".format(self.LOG_KEY, str(ex)))
            success = False
        return success

    def getmerge(self, hdfs_path, local_path, overwrite=False):
        """download file from hdfs and merge them into one file
        """
        if os.path.exists(local_path):
            if overwrite:
                os.remove(local_path)
                print("old '{}' has been deleted".format(local_path))
            else:
                print('%s already exists, no need to download again' % local_path)
                return False

        try:
            cmd = 'hadoop fs -getmerge %s %s' % (hdfs_path, local_path)
            success = self.shell(cmd)
        except Exception as ex:
            print("!!!!!! {} has error: {}".format(self.LOG_KEY, str(ex)))
            success = False

        if not success and os.path.exists(local_path):
            os.remove(local_path)

        success = success and os.path.exists(local_path)

        if success:
            print("SUCCEED download from '{}' to '{}'".format(hdfs_path, local_path))
        else:
            print("!!! FAILED download from '{}' to '{}'".format(hdfs_path, local_path))

        return success

    def getmerge_then_gunzip(self, hdfs_path, local_path):
        """getmerge gz file and then decompress
        """
        if os.path.exists(local_path):
            print('%s already exists, skip getmerge_then_gunzip' % local_path)
            return True

        local_path_compressed = local_path + ".gz"
        file_proc = FileProc()
        success = self.getmerge(hdfs_path, local_path_compressed)

        if not success:
            return False

        success = file_proc.gunzip(local_path_compressed)
        if not success:
            file_proc.rm_file(local_path)
            file_proc.rm_file(local_path_compressed)

        return success and file_proc.exists(local_path)

    def rmr(self, hdfs_path):
        """delete folder on hdfs
        """
        success = False
        try:
            # if not self.exists(hdfs_path):
            #     return True
            cmd = 'hadoop fs -rm -r %s' % (hdfs_path)
            success = self.shell(cmd)
        except Exception as ex:
            print("!!!!!! {} has error: {}".format(self.LOG_KEY, str(ex)))
            success = False
        return success

    def mkdir(self, hdfs_path):
        success = False
        try:
            if self.exists(hdfs_path):
                return True
            cmd = 'hadoop fs -mkdir %s' % (hdfs_path)
            success = self.shell(cmd)
        except Exception as ex:
            print("!!!!!! {} has error: {}".format(self.LOG_KEY, str(ex)))
            success = False
        return success

    def put(self, local_file, hdfs_parent, overwrite=False):
        # put a b,  will upload and rename a to b
        # put a b/, is the correct way, upload to b/a
        hdfs_dest = os.path.join(hdfs_parent, os.path.basename(local_file))
        if overwrite:
            self.rmr(hdfs_dest)

        try:
            # NOT upload to hdfs_parent, but upload to hdfs_dest
            # then the command works, no matter whether hdfs_parent ends with '\' or not
            cmd = 'hadoop fs -put %s %s' % (local_file, hdfs_dest)
            self.shell(cmd)
            return hdfs_dest

        except Exception as ex:
            print("!!!!!! {} has error: {}".format(self.LOG_KEY, str(ex)))
            return None

    def mv(self, src_path, dest_path):
        success = False
        try:
            cmd = 'hadoop fs -mv %s %s' % (src_path, dest_path)
            success = self.shell(cmd)
        except Exception as ex:
            print("!!!!!! {} has error: {}".format(self.LOG_KEY, str(ex)))
            success = False
        return success

    def list_files(self, pattern):
        result = self.shell_with_return("{} dfs -ls {}".format(self.hdfs_cmd, pattern))

        fnames = []
        for line in result.split('\n'):
            line = line.strip()
            if len(line) == 0:
                continue

            if line.startswith('Found'):
                continue  # list返回的第一行是Found xxx items

            segments = line.split()
            fnames.append(segments[-1])
        return fnames

    def get(self, remote_file, local_file):
        cmd = '{} dfs -get {} {}'.format(self.hdfs_cmd, remote_file, local_file)
        return self.shell(cmd)


class StreamingProc(ShellWrapper):
    def __init__(self, hadoop_home):
        ShellWrapper.__init__(self, "streaming_proc")
        self._hadoop_home = hadoop_home.rstrip("/")
        self._generic_options = []
        self._streaming_options = []

    def add_generic_option(self, _property, _value):
        _property = str(_property)
        _value = str(_value)
        if len(_property) == 0 or len(_value) == 0:
            raise Exception("generic option -D <property> and <value> must not be empty")
        self._generic_options.append([_property, _value])

    def add_streaming_option(self, _property, _value):
        _property = str(_property)
        _value = str(_value)
        if len(_property) == 0 or len(_value) == 0:
            raise Exception("streaming option <property> and <value> must not be empty")
        self._streaming_options.append([_property, _value])

    def clear(self):
        self._generic_options = []
        self._streaming_options = []

    def check_essentials(self):
        essentials = {"input": 1, "output": 1, "mapper": 1}
        for _property, _value in self._streaming_options:
            if _property in essentials:
                del essentials[_property]
        if len(essentials) > 0:
            raise Exception("missing streaming_options: %s" % essentials.keys())

    def build_cmd(self):
        self.check_essentials()
        cmd = "%s/bin/hadoop jar %s/share/hadoop/tools/lib/hadoop-streaming-2.6.4.jar" % (
            self._hadoop_home, self._hadoop_home)
        for _property, _value in self._generic_options:
            tmp_value = _value
            if " " in _value:
                tmp_value = "\"%s\"" % _value
            cmd += " -D %s=%s" % (_property, tmp_value)
        for _property, _value in self._streaming_options:
            tmp_value = _value
            if " " in _value:
                tmp_value = "\"%s\"" % _value
            cmd += " -%s %s" % (_property, tmp_value)
        return cmd

    def to_script(self):
        lines = ["%s/bin/hadoop jar %s/share/hadoop/tools/lib/hadoop-streaming-2.6.4.jar" % (
            self._hadoop_home, self._hadoop_home)]

        for _property, _value in self._generic_options:
            tmp_value = _value
            if " " in _value:
                tmp_value = "\"%s\"" % _value
            lines.append(" -D %s=%s" % (_property, tmp_value))
        for _property, _value in self._streaming_options:
            tmp_value = _value
            if " " in _value:
                tmp_value = "\"%s\"" % _value
            lines.append(" -%s %s" % (_property, tmp_value))

        return " \\\n".join(lines)

    def run(self):
        cmd = self.build_cmd()
        return self.shell(cmd)
