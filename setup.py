import codecs
import glob
import os
import re
import grpc_tools.protoc
from pkg_resources import parse_requirements
from setuptools import setup, find_packages
from setuptools.command.develop import develop
from setuptools.command.install import install


def proto_compile(output_path):
    cli_args = ['grpc_tools.protoc', '--proto_path=src/proto', f'--python_out={output_path}',
                f'--grpc_python_out={output_path}'] + glob.glob('src/proto/*.proto')
    code = grpc_tools.protoc.main(cli_args)
    if code:
        raise ValueError(f"{' '.join(cli_args)} finished with exit code {code}")
    for script in glob.iglob(f'{output_path}/*.py'):
        with open(script, 'r+')as file:
            code = file.read()
            file.seek(0)
            file.write(re.sub(r'\n(import .+_pb2.*)', 'from . \\1', code))
            file.truncate()


class ProtoCompileInstall(install):
    def run(self):
        proto_compile(os.path.join(self.build_lib, 'src', 'proto'))
        super().run()


class ProtoCompileDevelop(develop):
    def run(self):
        proto_compile(os.path.join('src', 'proto'))
        super().run()


here = os.path.abspath(os.path.dirname(__file__))
with open('requirements.txt')as requirements_file:
    install_requires = list(map(str, parse_requirements(requirements_file)))
with codecs.open(os.path.join(here, 'src/__init__.py'), encoding='utf-8')as init_file:
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", init_file.read(), re.M)
    version_string = version_match.group(1)
setup(name='src', version=version_string,
      cmdclass={'install': ProtoCompileInstall, 'develop': ProtoCompileDevelop}, packages=find_packages(),
      package_data={'src': ['proto/*']}, include_package_data=True, install_requires=install_requires,
      entry_points={'console_scripts': ['src-server = scripts.run_server:main', ]}, )
