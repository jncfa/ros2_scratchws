from setuptools import find_packages, setup

package_name = 'carmen2bag'

setup(
    name=package_name,
    version='0.0.0',
    packages=find_packages(exclude=['test']),
    data_files=[
        ('share/ament_index/resource_index/packages',
            ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
    ],
    install_requires=['setuptools', 'rich'],
    zip_safe=True,
    maintainer='ros',
    maintainer_email='jncfa@proton.me',
    description='TODO: Package description',
    license='MIT',
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [
            'carmen2bag = carmen2bag.carmen2bag:main'
        ],
    },
)
