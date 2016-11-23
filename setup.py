from setuptools import setup

setup(name='behem0th',
      version='0.1',
      description='File synchronisation done right.',
      url='https://github.com/robot0nfire/behem0th',
      author='robot0nfire',
      author_email='team@robot0nfire.com',
      license='MIT',
      packages=['behem0th'],
      install_requires=[
          'watchdog==0.8.3'
      ],
      zip_safe=False)
