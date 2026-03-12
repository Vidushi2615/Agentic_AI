from setuptools import setup


package_name = 'tb3_topic_nav'


setup(
    name=package_name,
    version='0.0.1',
    packages=[package_name],
    data_files=[
        ('share/ament_index/resource_index/packages', ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml', 'README.md']),
        ('share/' + package_name + '/config', ['config/nav2_burger.yaml']),
        ('share/' + package_name + '/launch', ['launch/sim_nav_bringup.launch.py']),
    ],
    install_requires=['setuptools'],
    zip_safe=True,
    maintainer='vidushi',
    maintainer_email='vidushi@example.com',
    description='TurtleBot3 Gazebo and Nav2 bringup with topic-driven navigation goals.',
    license='Apache-2.0',
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [
            'goal_pose_navigator = tb3_topic_nav.goal_pose_navigator:main',
        ],
    },
)
