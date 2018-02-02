from distutils.core import setup

setup(name='datadriven_basic_consumer',
      version='1.0',
      description='A generic RabbitMQ consumer',
      url='https://bitbucket.org/fbanke/basic_consumer',
      author='Frederik Banke',
      author_email='frederik@patch.dk',
      packages=['datadriven_basic_consumer'],
      install_requires=['pika'],
)
