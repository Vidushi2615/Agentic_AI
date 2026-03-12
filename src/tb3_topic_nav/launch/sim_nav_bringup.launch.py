import os

from ament_index_python.packages import get_package_share_directory
from launch import LaunchDescription
from launch.actions import ExecuteProcess, IncludeLaunchDescription, TimerAction
from launch.launch_description_sources import PythonLaunchDescriptionSource
from launch_ros.actions import Node


def generate_launch_description() -> LaunchDescription:
    initial_pose = (
        '{header: {frame_id: map}, pose: {pose: {position: {x: -2.0, y: -0.5, z: 0.0}, '
        'orientation: {x: 0.0, y: 0.0, z: 0.0, w: 1.0}}, covariance: [0.25, 0.0, 0.0, 0.0, 0.0, 0.0, '
        '0.0, 0.25, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, '
        '0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.1]}}'
    )
    package_share = get_package_share_directory('tb3_topic_nav')

    gazebo = IncludeLaunchDescription(
        PythonLaunchDescriptionSource(
            os.path.join(get_package_share_directory('turtlebot3_gazebo'), 'launch', 'turtlebot3_world.launch.py')
        )
    )

    nav2 = IncludeLaunchDescription(
        PythonLaunchDescriptionSource(
            os.path.join(get_package_share_directory('nav2_bringup'), 'launch', 'bringup_launch.py')
        ),
        launch_arguments={
            'map': os.path.join(get_package_share_directory('turtlebot3_navigation2'), 'map', 'map.yaml'),
            'use_sim_time': 'true',
            'autostart': 'true',
            'use_composition': 'False',
            'params_file': os.path.join(package_share, 'config', 'nav2_burger.yaml'),
        }.items(),
    )

    navigator = Node(
        package='tb3_topic_nav',
        executable='goal_pose_navigator',
        name='goal_pose_navigator',
        output='screen',
        parameters=[{'use_sim_time': True}],
    )

    publish_initial_pose = TimerAction(
        period=8.0,
        actions=[
            ExecuteProcess(
                cmd=[
                    'ros2',
                    'topic',
                    'pub',
                    '--once',
                    '/initialpose',
                    'geometry_msgs/msg/PoseWithCovarianceStamped',
                    initial_pose,
                ],
                output='screen',
            )
        ],
    )

    return LaunchDescription([gazebo, nav2, navigator, publish_initial_pose])
