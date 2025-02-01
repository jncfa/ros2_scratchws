from typing import Callable, NamedTuple
from rich.console import Console

from rclpy.qos import QoSProfile
from rclpy.qos import qos_profile_sensor_data as SensorDataQoS
from rclpy.qos import qos_profile_system_default as SystemDefaultQoS

from rclpy.serialization import serialize_message

from rclpy.time import Time as rclpyTime
from sensor_msgs.msg import LaserScan
from nav_msgs.msg import Odometry
from nmea_msgs.msg import Gpgga
from nmea_msgs.msg import Gprmc
from rcl_interfaces.msg import Parameter
from tf2_msgs.msg import TFMessage
from enum import StrEnum

class CARMENMsgTypes(StrEnum):
    PARAM = "PARAM"
    SYNC = "SYNC"
    ODOM = "ODOM"
    RAWLASER = "RAWLASER"
    ROBOTLASER = "ROBOTLASER"
    NMEAGGA = "NMEAGGA"
    NMEARMC = "NMEARMC"


class CARMENMsgBase(NamedTuple):
    msg_type: CARMENMsgTypes
    topic: str
    ipc_hostname: str 
    ipc_timestamp: float    
    logger_timestamp: float

class CARMENRawLaserMsg(CARMENMsgBase):
    msg_type: CARMENMsgTypes.RAWLASER
    laser_scan: LaserScan

class CARMENRobotLaserMsg(CARMENMsgBase):
    msg_type: CARMENMsgTypes.RAWLASER
    laser_scan: LaserScan
    tf: TFMessage

class CARMENParamMsg(CARMENMsgBase):
    msg_type: CARMENMsgTypes.PARAM
    param: Parameter


console = Console()

ParserCallback = Callable[[list[str], float, str, float], RosPartialMsg]
CompleteCallback = Callable[[list[str], float, str, float], RosMsg]

registered_parsers: dict[str, Callable[..., RosMsg]] = {}

def get_topic_name(key: str) -> str:
    return f"/{key.lower()}"

def register_parser(key: str, topic: str | None = None) -> Callable[[ParserCallback], CompleteCallback]:

    if topic is None:
        topic = get_topic_name(key)

    def push_func_to_parsers(func: ParserCallback) -> CompleteCallback:
        if key in registered_parsers:
            raise RuntimeError("Cannot have multiple parsers with the same key!")
        registered_parsers[key] = func

        def f(*args, **kwds):
            return RosMsg(*func(*args, **kwds))
        
        return func
    return push_func_to_parsers

def carmen2ros(input_ssv: str) -> RosMsg:
    # carmen msgs are: message_name [message contents] ipc_timestamp ipc_hostname logger_timestamp
    
    # split msg type from the rest so we can invoke the parser for this specific msg
    [msg_type, *args] = input_ssv.strip().split(' ')
    
    if (msg_type not in registered_parsers):
        raise LookupError(f"Could not find parser for message type {msg_type}")
    
    [ipc_timestamp, ipc_hostname, logger_timestamp] = args[-3:]

    try:
        return registered_parsers[msg_type](args[:-3], ipc_timestamp=float(ipc_timestamp), ipc_hostname=ipc_hostname, logger_timestamp=float(logger_timestamp))

    except:
        console.print_exception()

@register_parser('PARAM')
def param_parser(params: list[str], ipc_timestamp: float, ipc_hostname: str, logger_timestamp: float) -> RosPartialMsg:
    console.log("Logging locals", log_locals=True)

@register_parser('SYNC')
def sync_parser(params: list[str], ipc_timestamp: float, ipc_hostname: str, logger_timestamp: float) -> RosPartialMsg:
    console.log("Logging locals", log_locals=True)

@register_parser('ODOM')
def odom_parser(params: list[str], ipc_timestamp: float, ipc_hostname: str, logger_timestamp: float) -> RosPartialMsg:
    console.log("Logging locals", log_locals=True)

@register_parser('RAWLASER1')
@register_parser('RAWLASER2')
@register_parser('RAWLASER3')
@register_parser('RAWLASER4')
def rwlaser_parser(params: list[str], ipc_timestamp: float, ipc_hostname: str, logger_timestamp: float) -> RosPartialMsg:
    console.log("Logging locals", log_locals=True)

    # laser_type start_angle field_of_view angular_resolution 
    #   maximum_range accuracy remission_mode 
    #   num_readings [range_readings] num_remissions [remission values]

    (laser_type, start_angle, field_of_view, angular_resolution, maximum_range, accuracy, remission_mode, num_readings) = params[:8]
    num_readings = int(num_readings)
    range_readings = [float(k) for k in params[8:(8+num_readings)]]
    num_remissions = int(params[8+num_readings+1])
    remission_values = [float(k) for k in params[8+num_readings+1+num_remissions:]]
    
    console.log("Logging locals end", log_locals=True)

@register_parser('ROBOTLASER1')
@register_parser('ROBOTLASER2')
def robotlaser_parser(params: list[str], ipc_timestamp: float, ipc_hostname: str, logger_timestamp: float) -> RosPartialMsg:
# laser_type start_angle field_of_view angular_resolution
#   maximum_range accuracy remission_mode 
#   num_readings [range_readings] laser_pose_x laser_pose_y laser_pose_theta
#   robot_pose_x robot_pose_y robot_pose_theta 
#   laser_tv laser_rv forward_safety_dist side_safty_dist

    console.log("Logging locals", log_locals=True)

@register_parser('NMEAGGA')
def nmeagga_parser(params: list[str], ipc_timestamp: float, ipc_hostname: str, logger_timestamp: float) -> RosPartialMsg:
    # gpsnr utc latitude lat_orient longitude long_orient gps_quality 
    #   num_satellites hdop sea_level alititude geo_sea_level geo_sep data_age

    console.log("Logging locals", log_locals=True)

@register_parser('NMEARMC')
def nmearmc_parser(params: list[str], ipc_timestamp: float, ipc_hostname: str, logger_timestamp: float) -> RosPartialMsg:
    # gpsnr utc latitude lat_orient longitude long_orient gps_quality 
    #   num_satellites hdop sea_level alititude geo_sea_level geo_sep data_age

    console.log("Logging locals", log_locals=True)