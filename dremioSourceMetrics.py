#!/usr/bin/env python3
#
#
# Name         : dremioMetrics.py
# Description  : Script to push Prometheus metrics for Dremio clusters via Push Gateway
# Author       : Dremio
# Date         : May 10, 2020
# Version      : 1.0
# Notes        : Needs python client for Prometheus: Install client using "pip3 install prometheus_client"
#                Uses Python JDBC Module: Install client using "pip3 install JayDeBeApi"
# CHANGE LOG   :
#  Version 1.1 :
#          Date: June 8, 2020
#   Description: Added support for SSL. In addition, the script support clusters using HA and non-HA setup
#

import sys
import configparser
import os.path
import requests
import urllib3
import jaydebeapi
from prometheus_client import CollectorRegistry, Gauge, pushadd_to_gateway, instance_ip_grouping_key

# Configuration
debug = True
api_timeout = 60
jmxProtocol = "http://"
sys.tracebacklimit = 0
urllib3.disable_warnings(urllib3.exceptions.SecurityWarning)

# Push Gateway URL
pgwendpoint = "http://dremio-master:9091"

# Metrics
api_source_status_metric = 'dremio_api_source_status_Value'
sql_vds_count_value = 'dremio_sql_vds_count_Value'

# API endpoints
server_status = '/apiv2/server_status'
login_url = '/apiv2/login'
catalog_url = '/api/v3/catalog/'
pgw_api_url = '/api/v1/metrics'

def main():
	if len(sys.argv) < 3:
		print_usage()
	else:
		optionsFile = sys.argv[1]
		dremioCluster = sys.argv[2]

		# Parse options from the file
		cluster = configParser(optionsFile, dremioCluster)
		masterCoordinator = cluster['mastercoordinator']
		port = cluster['port']
		jmxPort = cluster['jmxport']
		username = cluster['username']
		password = cluster['password']
		jdbcPort = cluster['jdbcport']
		jdbcJar = cluster['jdbcjar']

		# Standby is an optional parameter
		standbyEnabled = False
		if 'standbycoordinator' in cluster:
			standbyCoordinator = cluster['standbycoordinator']
			standbyEnabled = True

		# SSL settings are optional. For SSL enabled clusters, we need the certificate bundle file too
		sslEnabled = False
		if 'sslenabled' in cluster:
			sslEnabled = cluster['sslenabled']
			protocol = 'https://'
			if 'sslcertlocation' in cluster:
				verifySsl = cluster['sslcertlocation']
			else:
				raise RuntimeError("SSL Configuration missing: Please set sslcertlocation option")
		else:
			protocol = 'http://'
			verifySsl = False

		# Master URL to hit
		masterUrl = protocol + masterCoordinator + ":" + str(port)

		# Check Coordinators
		status = 1
		activeCoordinator = masterCoordinator

		try:
			response = requests.get(masterUrl + server_status, timeout=api_timeout, verify=verifySsl)
		except requests.ConnectionError:
			if debug:
				print('Master Connection Error: ', response.json())
			if standbyEnabled:
				activeCoordinator = standbyCoordinator
				standbyUrl = protocol + standbyCoordinator + ":" + str(port)
				try:
					response = requests.get(standbyUrl + server_status, timeout=api_timeout, verify=verifySsl)
				except requests.ConnectionError:
					if debug:
						print('Standby Connection Error: ', response.json())
					status = 0
			else:
				status = 0

		if debug:
			print("activeCoordinator: ", activeCoordinator)
			print("status: ", status)

		if status == 0:
			# All the coordinators are down
			if debug:
				print("Coordinators are Down!")
			# Reset relevant metrics
			endpoint = pgwendpoint + pgw_api_url
			response = requests.get(endpoint)
			if response.status_code != 200:
				raise RuntimeError("API Error " + str(response.status_code)+ " - " + get_error_message(response))
			items = response.json()['data']
			for item in items:
				# Check if we have relevant metrics
				if api_source_status_metric in item:
					for record in item.items():
						if api_source_status_metric in record:
							metric = dict(dict(record[1])['metrics'][0])
							jobName = metric['labels']['job']
							sourceType = metric['labels']['type']
							if jobName == dremioCluster:
								push_source_status_metric(dremioCluster, sourceName, 400)
				if sql_vds_count_value in item:
					for record in item.items():
						if sql_vds_count_value in record:
							metric = dict(dict(record[1])['metrics'][0])
							jobName = metric['labels']['job']
							if jobName == dremioCluster:
								push_sql_metric(sql_vds_count_value, dremioCluster, 0)
		else:
			endpoint = protocol + activeCoordinator + ":" + str(port)

			headers = {"Content-Type": "application/json"}
			payload = '{"userName": "' + username + '","password": "' + password + '"}'
			response = requests.request("POST", endpoint + login_url, data=payload, headers=headers, timeout=api_timeout, verify=verifySsl)
			if response.status_code != 200:
				raise RuntimeError("Authentication error." + str(response.status_code))
			authtoken = '_dremio' + response.json()['token']
			headers = {"Content-Type": "application/json", "Authorization": authtoken}
			# Get Catalog
			response = requests.request("GET", endpoint + catalog_url, headers=headers, timeout=api_timeout, verify=verifySsl)
			if response.status_code != 200:
				raise RuntimeError("API Error " + str(response.status_code)+ " - " + get_error_message(response))
			containers = response.json()['data']
			for container in containers:
				if container['containerType'] == 'SOURCE':
					# Validate Source Status
					response = requests.request("GET", endpoint + catalog_url + container['id'], headers=headers, timeout=api_timeout, verify=verifySsl)
					if debug:
						print("\nContainer: ", response.json())

					name = container['path'][0]
					report_data_source_status (name, response.status_code)
					push_source_status_metric(dremioCluster, name, response.status_code)

			# SQL Metrics
			# Connect to Dremio Node
			jdbcUrl = "jdbc:dremio:direct=" + activeCoordinator + ":" + str(jdbcPort)
			cnxn = jaydebeapi.connect("com.dremio.jdbc.Driver", jdbcUrl, [username, password], jdbcJar)
			cursor = cnxn.cursor()

			# VDS Count
			query = 'select count(*) from information_schema."TABLES" where table_type = \'VIEW\' and table_schema not like \'@%\''
			cursor.execute(query)
			result = cursor.fetchall()
			for row in result:
				vdsCount = row[0]
				push_sql_metric(sql_vds_count_value, dremioCluster, vdsCount)

def push_source_status_metric(dremioCluster, sourceName, status):
	# Push Coordinator Status
	registry = CollectorRegistry()
	metric = Gauge(api_source_status_metric, "Source status, pushed via Gateway", labelnames=['source'], registry=registry)
	metric.labels(sourceName).set_to_current_time()
	metric.labels(sourceName).set(status)
	groupingKey = dict({"job": dremioCluster, "source": sourceName})
	pushadd_to_gateway(pgwendpoint, job=dremioCluster, registry=registry, timeout=api_timeout, grouping_key=groupingKey)

def push_sql_metric(metricName, dremioCluster, metricValue):
	# Push SQL Metric
	registry = CollectorRegistry()
	metric = Gauge(metricName, "SQL Metric, pushed via Gateway", registry=registry)
	metric.set_to_current_time()
	metric.set(metricValue)
	pushadd_to_gateway(pgwendpoint, job=dremioCluster, registry=registry, timeout=api_timeout)

def report_data_source_status(name, status):
	print(str(name) + '-' + str(status))

def configParser(configFile, section):
	config = configparser.ConfigParser()
	config.read(configFile)
	configDict = {}
	options = config.options(section)
	for option in options:
		try:
			configDict[option] = config.get(section, option)
		except:
			print("exception on %s!" % option)
			configDict[option] = None
	return configDict

def get_error_message(self, response):
	message = ""
	try:
		if 'errorMessage' in response.json():
			message = message + " errorMessage: " + str(response.json()['errorMessage'])
		if 'moreInfo' in response.json():
			message = message + " moreInfo: " + str(response.json()['moreInfo'])
	except:
		message = message + " content: " + str(response.content)
	return message

def print_usage():
	prog = os.path.basename(__file__)
	print("USAGE: " + prog + " <Options File> <Dremio Cluster Name>")
	print("   Eg: " + prog + " /home/dremio/dremio.ini dremioCluster\n")

if __name__ == "__main__":
	main()
