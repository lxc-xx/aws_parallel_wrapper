#!/usr/bin/env python
import os.path
import boto
import time
import socket
from datetime import datetime
import boto.ec2
import sys
import os

#Please replace the following with your own configuration
region = 'Your Region'
aws_access_key_id='your_aws_access_key_id'
aws_secret_access_key='your_secret_access_key'
key_file = 'your_key_file_path'
key_name = 'your_key_name'
security_group = 'your_security_group_name'
ami_id = 'your_ami_id'

#def is_initilized(inst):
#    result = False
#    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#    if inst.public_dns_name == "":
#        return False
#    try: 
#        s.connect((inst.public_dns_name, 22)) 
#        result=True
#    except socket.error as e: 
#        print "Error on connect: %s" % e
#        result=False
#
#    s.close()
#
#    return result


def main(argv):
    if len(argv) != 6:
        print "Usage: parallel_wrapper.py cmds_per_instance proc_per_instance max_hours instance_type cmds_list job_name"
        sys.exit(1)

    cmds_per_inst = int(argv[0])
    proc_per_inst = int(argv[1])
    max_hour = int(argv[2])
    instance_type = argv[3]
    cmd_path = os.path.abspath(argv[4])
    job_name = argv[5]

    line_num = len(open(cmd_path).readlines())

    #boto.set_stream_logger('boto')

    conn = boto.ec2.connect_to_region(region, aws_access_key_id = aws_access_key_id, aws_secret_access_key = aws_secret_access_key)
    if conn == None:
        print "Connection failed"
        sys.exit(1)

    start = 0

    inst_list = []
    inst_id_list = []
    cmd_list = []
    job_idx = 0
    job_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    while start < line_num:
        head_num = min(start + cmds_per_inst, line_num)
       
        tail_num = head_num - start

        #mount_cmd = "source ~/.bashrc"
        crop_cmd = "head -"+str(head_num)+ " " + cmd_path + " | tail -" + str(tail_num)

        start = head_num

        xargs_cmd = "xargs -L 1 -I {} -P " + str(proc_per_inst) + " dumb.py {}"
        shut_down = " sleep 20 && sudo halt"
        time_limit_cmd = "(sleep " + str(max_hour) + "h && sudo halt&)"

        final_cmd = time_limit_cmd + "; " + crop_cmd + " | " + xargs_cmd + " && " + shut_down
        #final_cmd = time_limit_cmd + "; " + crop_cmd + " | " + xargs_cmd
    
        cmd_list.append(final_cmd)
        #print final_cmd

        resv = conn.run_instances(ami_id,key_name=key_name, instance_type=instance_type, security_groups=[security_group],instance_initiated_shutdown_behavior="terminate")
        inst = resv.instances[0]
        conn.create_tags([inst.id], {"Name": "[Parallel_Job %s]: " % job_time + job_name + "-" + str(job_idx)})
        job_idx = job_idx + 1

        inst_list.append(inst)
        inst_id_list.append(inst.id)
    

    if len(inst_list) == 0:
        return 1

    while True:
        #break
        #print inst_id_list
        done_num = 0
        print "=== Instances Status Check %s ===" % datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for stat in conn.get_all_instance_status(inst_id_list):
            if str(stat.system_status) == "Status:ok" and str(stat.instance_status) == "Status:ok":
                print str(stat.id) + ": Initialized"
                done_num = done_num + 1
            else:
                print str(stat.id) + ": Initializing"
        
        if done_num == len(inst_list):
            break
        time.sleep(5)

    cmd_idx = 0

    print "Deploying jobs..."
    for resv in conn.get_all_instances(inst_id_list):
        inst = resv.instances[0]
        host = inst.public_dns_name

        log_file = os.getcwd() + "/" + job_name + "-" + str(cmd_idx) + ".log"
        err_file = os.getcwd() + "/" + job_name + "-" + str(cmd_idx) + ".err"

        ssh_cmd = "ssh -o StrictHostKeyChecking=no -i " + key_file + " ubuntu@" + host + ' \"set -o pipefail && bash -ic \\"(%s) >' % cmd_list[cmd_idx] +log_file +' 2> ' + err_file + ' &\\"\"'
        cmd_idx = cmd_idx + 1

        os.system(ssh_cmd)
        #print ssh_cmd

    return 0

if __name__ == "__main__":
    main(sys.argv[1:])
