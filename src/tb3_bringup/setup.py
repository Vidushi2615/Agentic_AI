from setuptools import setup


package_name = 'tb3_bringup'


setup(
    name=package_name,
    version='0.0.1',
    packages=[package_name],
    data_files=[
        ('share/ament_index/resource_index/packages', ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml', 'README.md']),
        ('share/' + package_name + '/launch', ['launch/house_nav_bringup.launch.py']),
        ('share/' + package_name + '/rviz', ['rviz/house_nav_debug.rviz']),
    ],
    install_requires=['setuptools'],
    zip_safe=True,
    maintainer='vidushi',
    maintainer_email='vidushi@example.com',
    description='TurtleBot3 house-world Gazebo and Nav2 bringup package.',
    license='Apache-2.0',
    tests_require=['pytest'],
    entry_points={},
)
