from setuptools import setup


package_name = 'tb3_agent'


setup(
    name=package_name,
    version='0.0.1',
    packages=[package_name],
    data_files=[
        ('share/ament_index/resource_index/packages', ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml', 'README.md']),
        ('share/' + package_name + '/config', ['config/locations.yaml']),
        ('share/' + package_name + '/launch', ['launch/agent_bringup.launch.py']),
    ],
    install_requires=['setuptools'],
    zip_safe=True,
    maintainer='vidushi',
    maintainer_email='vidushi@example.com',
    description='TurtleBot3 agent-side command execution package.',
    license='Apache-2.0',
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [
            'command_executor_node = tb3_agent.command_executor_node:main',
            'task_agent_node = tb3_agent.task_agent_node:main',
        ],
    },
)
