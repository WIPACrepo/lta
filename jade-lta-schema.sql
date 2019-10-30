-- MySQL dump 10.13  Distrib 5.1.73, for redhat-linux-gnu (x86_64)
--
-- Host: jade-lta-db-test    Database: jade-lta
-- ------------------------------------------------------
-- Server version	5.1.73

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `jade_bundle`
--

DROP TABLE IF EXISTS `jade_bundle`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jade_bundle` (
  `jade_bundle_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `bundle_file` varchar(255) DEFAULT NULL,
  `capacity` bigint(20) DEFAULT NULL,
  `checksum` varchar(255) DEFAULT NULL,
  `closed` bit(1) DEFAULT NULL,
  `date_created` datetime DEFAULT NULL,
  `date_updated` datetime DEFAULT NULL,
  `destination` varchar(255) DEFAULT NULL,
  `reference_uuid` char(36) DEFAULT NULL,
  `size` bigint(20) DEFAULT NULL,
  `uuid` char(36) DEFAULT NULL,
  `version` bigint(20) DEFAULT NULL,
  `jade_host_id` bigint(20) DEFAULT NULL,
  `extension` bit(1) DEFAULT NULL,
  `jade_parent_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`jade_bundle_id`),
  KEY `FK_4wm1dv3egi7nfifndv5rkdmvf` (`jade_host_id`),
  CONSTRAINT `FK_4wm1dv3egi7nfifndv5rkdmvf` FOREIGN KEY (`jade_host_id`) REFERENCES `jade_host` (`jade_host_id`)
) ENGINE=InnoDB AUTO_INCREMENT=3023 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jade_credentials`
--

DROP TABLE IF EXISTS `jade_credentials`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jade_credentials` (
  `jade_credentials_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `date_created` datetime DEFAULT NULL,
  `date_updated` datetime DEFAULT NULL,
  `password` varchar(255) DEFAULT NULL,
  `ssh_key_path` varchar(255) DEFAULT NULL,
  `username` varchar(255) DEFAULT NULL,
  `uuid` char(36) DEFAULT NULL,
  `version` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`jade_credentials_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jade_data_stream`
--

DROP TABLE IF EXISTS `jade_data_stream`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jade_data_stream` (
  `jade_data_stream_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `active` bit(1) DEFAULT NULL,
  `binary_suffix` varchar(255) DEFAULT NULL,
  `calculate_ingest_checksum` bit(1) DEFAULT NULL,
  `compression` varchar(255) DEFAULT NULL,
  `date_created` datetime DEFAULT NULL,
  `date_updated` datetime DEFAULT NULL,
  `file_host` varchar(255) DEFAULT NULL,
  `file_path` varchar(255) DEFAULT NULL,
  `file_prefix` varchar(255) DEFAULT NULL,
  `repeat_seconds` int(11) DEFAULT NULL,
  `satellite` bit(1) DEFAULT NULL,
  `semaphore_suffix` varchar(255) DEFAULT NULL,
  `uuid` char(36) DEFAULT NULL,
  `verify_remote_checksum` bit(1) DEFAULT NULL,
  `verify_remote_length` bit(1) DEFAULT NULL,
  `version` bigint(20) DEFAULT NULL,
  `workflow_bean` varchar(255) DEFAULT NULL,
  `xfer_limit_kbits_sec` bigint(20) DEFAULT NULL,
  `jade_credentials_id` bigint(20) DEFAULT NULL,
  `jade_stream_metadata_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`jade_data_stream_id`),
  KEY `FK_cm36vt8lee0ilcebqyw8rep8g` (`jade_credentials_id`),
  KEY `FK_gxjif5bss06l654ff17fcvrnj` (`jade_stream_metadata_id`),
  CONSTRAINT `FK_cm36vt8lee0ilcebqyw8rep8g` FOREIGN KEY (`jade_credentials_id`) REFERENCES `jade_credentials` (`jade_credentials_id`),
  CONSTRAINT `FK_gxjif5bss06l654ff17fcvrnj` FOREIGN KEY (`jade_stream_metadata_id`) REFERENCES `jade_stream_metadata` (`jade_stream_metadata_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jade_data_stream_metrics`
--

DROP TABLE IF EXISTS `jade_data_stream_metrics`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jade_data_stream_metrics` (
  `jade_data_stream_metrics_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `data_stream_uuid` varchar(255) DEFAULT NULL,
  `date_created` datetime DEFAULT NULL,
  `date_updated` datetime DEFAULT NULL,
  `file_count` bigint(20) DEFAULT NULL,
  `file_pair_count` bigint(20) DEFAULT NULL,
  `file_pair_size` bigint(20) DEFAULT NULL,
  `file_size` bigint(20) DEFAULT NULL,
  `date_oldest_file` datetime DEFAULT NULL,
  `uuid` char(36) DEFAULT NULL,
  `version` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`jade_data_stream_metrics_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jade_disk`
--

DROP TABLE IF EXISTS `jade_disk`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jade_disk` (
  `jade_disk_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `bad` bit(1) DEFAULT NULL,
  `capacity` bigint(20) DEFAULT NULL,
  `closed` bit(1) DEFAULT NULL,
  `copy_id` int(11) DEFAULT NULL,
  `date_created` datetime DEFAULT NULL,
  `date_updated` datetime DEFAULT NULL,
  `device_path` varchar(255) DEFAULT NULL,
  `label` varchar(255) DEFAULT NULL,
  `on_hold` bit(1) DEFAULT NULL,
  `uuid` char(36) DEFAULT NULL,
  `version` bigint(20) DEFAULT NULL,
  `jade_host_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`jade_disk_id`),
  KEY `FK_iqojtoh4ipbnuldvgq4hvwpge` (`jade_host_id`),
  CONSTRAINT `FK_iqojtoh4ipbnuldvgq4hvwpge` FOREIGN KEY (`jade_host_id`) REFERENCES `jade_host` (`jade_host_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jade_disk_archival_record`
--

DROP TABLE IF EXISTS `jade_disk_archival_record`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jade_disk_archival_record` (
  `jade_disk_archival_record_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `date_created` datetime DEFAULT NULL,
  `date_updated` datetime DEFAULT NULL,
  `uuid` char(36) DEFAULT NULL,
  `version` bigint(20) DEFAULT NULL,
  `jade_disk_id` bigint(20) DEFAULT NULL,
  `jade_file_pair_id` bigint(20) DEFAULT NULL,
  `jade_host_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`jade_disk_archival_record_id`),
  KEY `FK_lamh8o1ougd1begi2kfyb7vjj` (`jade_disk_id`),
  KEY `FK_n4fjij0sr9q9lwbbuv28ic3sf` (`jade_file_pair_id`),
  KEY `FK_reklychsmgvh9m8qmot08504p` (`jade_host_id`),
  CONSTRAINT `FK_lamh8o1ougd1begi2kfyb7vjj` FOREIGN KEY (`jade_disk_id`) REFERENCES `jade_disk` (`jade_disk_id`),
  CONSTRAINT `FK_n4fjij0sr9q9lwbbuv28ic3sf` FOREIGN KEY (`jade_file_pair_id`) REFERENCES `jade_file_pair` (`jade_file_pair_id`),
  CONSTRAINT `FK_reklychsmgvh9m8qmot08504p` FOREIGN KEY (`jade_host_id`) REFERENCES `jade_host` (`jade_host_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jade_disk_archive`
--

DROP TABLE IF EXISTS `jade_disk_archive`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jade_disk_archive` (
  `jade_disk_archive_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `capacity` bigint(20) DEFAULT NULL,
  `date_created` datetime DEFAULT NULL,
  `date_updated` datetime DEFAULT NULL,
  `description` varchar(255) DEFAULT NULL,
  `number_of_copies` int(11) DEFAULT NULL,
  `uuid` char(36) DEFAULT NULL,
  `version` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`jade_disk_archive_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jade_file_pair`
--

DROP TABLE IF EXISTS `jade_file_pair`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jade_file_pair` (
  `jade_file_pair_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `archive_checksum` varchar(255) DEFAULT NULL,
  `archive_file` varchar(255) DEFAULT NULL,
  `archive_size` bigint(20) DEFAULT NULL,
  `binary_file` varchar(255) DEFAULT NULL,
  `binary_size` bigint(20) DEFAULT NULL,
  `data_stream_uuid` varchar(255) DEFAULT NULL,
  `date_archived` datetime DEFAULT NULL,
  `date_created` datetime DEFAULT NULL,
  `date_fetched` datetime DEFAULT NULL,
  `date_processed` datetime DEFAULT NULL,
  `date_updated` datetime DEFAULT NULL,
  `date_verified` datetime DEFAULT NULL,
  `fetch_checksum` varchar(255) DEFAULT NULL,
  `fingerprint` varchar(255) DEFAULT NULL,
  `ingest_checksum` bigint(20) DEFAULT NULL,
  `metadata_file` varchar(255) DEFAULT NULL,
  `origin_checksum` varchar(255) DEFAULT NULL,
  `date_modified_origin` datetime DEFAULT NULL,
  `semaphore_file` varchar(255) DEFAULT NULL,
  `uuid` char(36) DEFAULT NULL,
  `version` bigint(20) DEFAULT NULL,
  `archived_by_host_id` bigint(20) DEFAULT NULL,
  `fetched_by_host_id` bigint(20) DEFAULT NULL,
  `processed_by_host_id` bigint(20) DEFAULT NULL,
  `verified_by_host_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`jade_file_pair_id`),
  UNIQUE KEY `fingerprint_index` (`fingerprint`),
  KEY `FK_qowgco4mwdcv86nyurmey09th` (`archived_by_host_id`),
  KEY `FK_rauaxj6vxjia50q7tcyfaw24s` (`fetched_by_host_id`),
  KEY `FK_19eb858iaypksh0r1s5anncbq` (`processed_by_host_id`),
  KEY `FK_4of17m0dfh6k6q3ltbcyelhud` (`verified_by_host_id`),
  CONSTRAINT `FK_19eb858iaypksh0r1s5anncbq` FOREIGN KEY (`processed_by_host_id`) REFERENCES `jade_host` (`jade_host_id`),
  CONSTRAINT `FK_4of17m0dfh6k6q3ltbcyelhud` FOREIGN KEY (`verified_by_host_id`) REFERENCES `jade_host` (`jade_host_id`),
  CONSTRAINT `FK_qowgco4mwdcv86nyurmey09th` FOREIGN KEY (`archived_by_host_id`) REFERENCES `jade_host` (`jade_host_id`),
  CONSTRAINT `FK_rauaxj6vxjia50q7tcyfaw24s` FOREIGN KEY (`fetched_by_host_id`) REFERENCES `jade_host` (`jade_host_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jade_file_pair_metadata`
--

DROP TABLE IF EXISTS `jade_file_pair_metadata`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jade_file_pair_metadata` (
  `jade_file_pair_metadata_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `date_created` datetime DEFAULT NULL,
  `date_updated` datetime DEFAULT NULL,
  `uuid` char(36) DEFAULT NULL,
  `version` bigint(20) DEFAULT NULL,
  `xml_checksum` varchar(255) DEFAULT NULL,
  `xml_metadata` text,
  `jade_file_pair_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`jade_file_pair_metadata_id`),
  KEY `FK_f0k651geqicn5jrj5rc0r540c` (`jade_file_pair_id`),
  CONSTRAINT `FK_f0k651geqicn5jrj5rc0r540c` FOREIGN KEY (`jade_file_pair_id`) REFERENCES `jade_file_pair` (`jade_file_pair_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jade_globus_archive`
--

DROP TABLE IF EXISTS `jade_globus_archive`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jade_globus_archive` (
  `jade_globus_archive_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `active` bit(1) DEFAULT NULL,
  `date_created` datetime DEFAULT NULL,
  `date_updated` datetime DEFAULT NULL,
  `description` varchar(255) DEFAULT NULL,
  `uuid` char(36) DEFAULT NULL,
  `verification_path` varchar(255) DEFAULT NULL,
  `version` bigint(20) DEFAULT NULL,
  `destination_endpoint_id` bigint(20) DEFAULT NULL,
  `source_endpoint_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`jade_globus_archive_id`),
  KEY `FK_91pmyl4inq18ghyy6vndol77c` (`destination_endpoint_id`),
  KEY `FK_dkh9d1ucrj3leq25u63yg6ul3` (`source_endpoint_id`),
  CONSTRAINT `FK_91pmyl4inq18ghyy6vndol77c` FOREIGN KEY (`destination_endpoint_id`) REFERENCES `jade_globus_endpoint` (`jade_globus_endpoint_id`),
  CONSTRAINT `FK_dkh9d1ucrj3leq25u63yg6ul3` FOREIGN KEY (`source_endpoint_id`) REFERENCES `jade_globus_endpoint` (`jade_globus_endpoint_id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jade_globus_delete`
--

DROP TABLE IF EXISTS `jade_globus_delete`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jade_globus_delete` (
  `jade_globus_delete_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `code` varchar(255) DEFAULT NULL,
  `data_type` varchar(255) DEFAULT NULL,
  `date_created` datetime DEFAULT NULL,
  `date_updated` datetime DEFAULT NULL,
  `disposition` varchar(255) DEFAULT NULL,
  `failure_id` bigint(20) DEFAULT NULL,
  `message` varchar(255) DEFAULT NULL,
  `request_id` varchar(255) DEFAULT NULL,
  `resource` varchar(255) DEFAULT NULL,
  `result_json` longtext,
  `submission_id` varchar(255) DEFAULT NULL,
  `task_id` varchar(255) DEFAULT NULL,
  `uuid` char(36) DEFAULT NULL,
  `version` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`jade_globus_delete_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jade_globus_endpoint`
--

DROP TABLE IF EXISTS `jade_globus_endpoint`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jade_globus_endpoint` (
  `jade_globus_endpoint_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `date_created` datetime DEFAULT NULL,
  `date_updated` datetime DEFAULT NULL,
  `directory` varchar(255) DEFAULT NULL,
  `display_name` varchar(255) DEFAULT NULL,
  `uuid` char(36) DEFAULT NULL,
  `version` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`jade_globus_endpoint_id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jade_globus_file`
--

DROP TABLE IF EXISTS `jade_globus_file`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jade_globus_file` (
  `jade_globus_file_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `date_created` datetime DEFAULT NULL,
  `date_modified` datetime DEFAULT NULL,
  `date_updated` datetime DEFAULT NULL,
  `destination` varchar(255) DEFAULT NULL,
  `globus_endpoint_uuid` varchar(255) DEFAULT NULL,
  `file_name` varchar(255) DEFAULT NULL,
  `file_size` bigint(20) DEFAULT NULL,
  `uuid` char(36) DEFAULT NULL,
  `version` bigint(20) DEFAULT NULL,
  `jade_location_id` bigint(20) DEFAULT NULL,
  `jade_globus_transfer_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`jade_globus_file_id`),
  KEY `FK_f64v36t56mriu6ogbs94uctpn` (`jade_location_id`),
  KEY `FK_cfqekec0c47mf6tx76lom2o98` (`jade_globus_transfer_id`),
  CONSTRAINT `FK_cfqekec0c47mf6tx76lom2o98` FOREIGN KEY (`jade_globus_transfer_id`) REFERENCES `jade_globus_transfer` (`jade_globus_transfer_id`),
  CONSTRAINT `FK_f64v36t56mriu6ogbs94uctpn` FOREIGN KEY (`jade_location_id`) REFERENCES `jade_location` (`jade_location_id`)
) ENGINE=InnoDB AUTO_INCREMENT=4684 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jade_globus_transfer`
--

DROP TABLE IF EXISTS `jade_globus_transfer`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jade_globus_transfer` (
  `jade_globus_transfer_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `code` varchar(255) DEFAULT NULL,
  `data_type` varchar(255) DEFAULT NULL,
  `date_created` datetime DEFAULT NULL,
  `date_updated` datetime DEFAULT NULL,
  `disposition` varchar(255) DEFAULT NULL,
  `failure_id` bigint(20) DEFAULT NULL,
  `message` varchar(255) DEFAULT NULL,
  `request_id` varchar(255) DEFAULT NULL,
  `resource` varchar(255) DEFAULT NULL,
  `submission_id` varchar(255) DEFAULT NULL,
  `task_id` varchar(255) DEFAULT NULL,
  `uuid` char(36) DEFAULT NULL,
  `version` bigint(20) DEFAULT NULL,
  `jade_bundle_id` bigint(20) DEFAULT NULL,
  `jade_location_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`jade_globus_transfer_id`),
  KEY `FK_75hgiprmbd596hvn1fg2copow` (`jade_bundle_id`),
  KEY `FK_r593w91s3r7wp5ou8mrby6k6w` (`jade_location_id`),
  CONSTRAINT `FK_75hgiprmbd596hvn1fg2copow` FOREIGN KEY (`jade_bundle_id`) REFERENCES `jade_bundle` (`jade_bundle_id`),
  CONSTRAINT `FK_r593w91s3r7wp5ou8mrby6k6w` FOREIGN KEY (`jade_location_id`) REFERENCES `jade_location` (`jade_location_id`)
) ENGINE=InnoDB AUTO_INCREMENT=4888 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jade_host`
--

DROP TABLE IF EXISTS `jade_host`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jade_host` (
  `jade_host_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `allow_job_claim` bit(1) DEFAULT NULL,
  `allow_job_work` bit(1) DEFAULT NULL,
  `allow_open_job_claim` bit(1) DEFAULT NULL,
  `date_created` datetime DEFAULT NULL,
  `date_heartbeat` datetime DEFAULT NULL,
  `date_updated` datetime DEFAULT NULL,
  `host_name` varchar(255) DEFAULT NULL,
  `satellite_capable` bit(1) DEFAULT NULL,
  `uuid` char(36) DEFAULT NULL,
  `version` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`jade_host_id`),
  UNIQUE KEY `host_name_index` (`host_name`)
) ENGINE=InnoDB AUTO_INCREMENT=234 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jade_location`
--

DROP TABLE IF EXISTS `jade_location`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jade_location` (
  `jade_location_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `date_created` datetime DEFAULT NULL,
  `date_updated` datetime DEFAULT NULL,
  `display_name` varchar(255) DEFAULT NULL,
  `uuid` char(36) DEFAULT NULL,
  `version` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`jade_location_id`)
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jade_map_bundle_to_file_pair`
--

DROP TABLE IF EXISTS `jade_map_bundle_to_file_pair`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jade_map_bundle_to_file_pair` (
  `jade_bundle_id` bigint(20) NOT NULL,
  `jade_file_pair_id` bigint(20) NOT NULL,
  `jade_file_pair_order` int(11) NOT NULL,
  PRIMARY KEY (`jade_bundle_id`,`jade_file_pair_order`),
  KEY `FK_3do6hjuc5c662fvf7as25mrky` (`jade_file_pair_id`),
  CONSTRAINT `FK_3do6hjuc5c662fvf7as25mrky` FOREIGN KEY (`jade_file_pair_id`) REFERENCES `jade_file_pair` (`jade_file_pair_id`),
  CONSTRAINT `FK_h4qwqby2rqppuff6b642qqal9` FOREIGN KEY (`jade_bundle_id`) REFERENCES `jade_bundle` (`jade_bundle_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jade_map_bundle_to_location`
--

DROP TABLE IF EXISTS `jade_map_bundle_to_location`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jade_map_bundle_to_location` (
  `jade_bundle_id` bigint(20) NOT NULL,
  `jade_location_id` bigint(20) NOT NULL,
  `jade_location_order` int(11) NOT NULL,
  PRIMARY KEY (`jade_bundle_id`,`jade_location_order`),
  KEY `FK_9vqmivpn7rs9ctff6lyeeeroo` (`jade_location_id`),
  CONSTRAINT `FK_9vqmivpn7rs9ctff6lyeeeroo` FOREIGN KEY (`jade_location_id`) REFERENCES `jade_location` (`jade_location_id`),
  CONSTRAINT `FK_kyo1q49lpvay4r50hhuupcieo` FOREIGN KEY (`jade_bundle_id`) REFERENCES `jade_bundle` (`jade_bundle_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jade_map_bundle_to_mirror_request`
--

DROP TABLE IF EXISTS `jade_map_bundle_to_mirror_request`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jade_map_bundle_to_mirror_request` (
  `jade_mirror_request_id` bigint(20) NOT NULL,
  `jade_bundle_id` bigint(20) NOT NULL,
  `jade_bundle_order` int(11) NOT NULL,
  PRIMARY KEY (`jade_mirror_request_id`,`jade_bundle_order`),
  KEY `FK_gn2jobs8ratsabgxj9vobyaea` (`jade_bundle_id`),
  CONSTRAINT `FK_bapxhlrw94muv8ijmkuqcbj1c` FOREIGN KEY (`jade_mirror_request_id`) REFERENCES `jade_mirror_request` (`jade_mirror_request_id`),
  CONSTRAINT `FK_gn2jobs8ratsabgxj9vobyaea` FOREIGN KEY (`jade_bundle_id`) REFERENCES `jade_bundle` (`jade_bundle_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jade_map_bundle_to_picker_file`
--

DROP TABLE IF EXISTS `jade_map_bundle_to_picker_file`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jade_map_bundle_to_picker_file` (
  `jade_bundle_id` bigint(20) NOT NULL,
  `jade_picker_file_id` bigint(20) NOT NULL,
  `jade_picker_file_order` int(11) NOT NULL,
  PRIMARY KEY (`jade_bundle_id`,`jade_picker_file_order`),
  KEY `FK_hq0odeeuh630wn075q77lbbt3` (`jade_picker_file_id`),
  CONSTRAINT `FK_hq0odeeuh630wn075q77lbbt3` FOREIGN KEY (`jade_picker_file_id`) REFERENCES `jade_picker_file` (`jade_picker_file_id`),
  CONSTRAINT `FK_mptklr73chrmwb4mnd30fpaw5` FOREIGN KEY (`jade_bundle_id`) REFERENCES `jade_bundle` (`jade_bundle_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jade_map_bundle_to_warehouse_file`
--

DROP TABLE IF EXISTS `jade_map_bundle_to_warehouse_file`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jade_map_bundle_to_warehouse_file` (
  `jade_bundle_id` bigint(20) NOT NULL,
  `jade_warehouse_file_id` bigint(20) NOT NULL,
  `jade_warehouse_file_order` int(11) NOT NULL,
  PRIMARY KEY (`jade_bundle_id`,`jade_warehouse_file_order`),
  KEY `FK_m4g2142eo7ejivx7q2f9p8nwc` (`jade_warehouse_file_id`),
  CONSTRAINT `FK_lrdcwup7oevm7vqrn8wlfs9o5` FOREIGN KEY (`jade_bundle_id`) REFERENCES `jade_bundle` (`jade_bundle_id`),
  CONSTRAINT `FK_m4g2142eo7ejivx7q2f9p8nwc` FOREIGN KEY (`jade_warehouse_file_id`) REFERENCES `jade_warehouse_file` (`jade_warehouse_file_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jade_map_data_stream_to_disk_archive`
--

DROP TABLE IF EXISTS `jade_map_data_stream_to_disk_archive`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jade_map_data_stream_to_disk_archive` (
  `jade_data_stream_id` bigint(20) NOT NULL,
  `jade_disk_archive_id` bigint(20) NOT NULL,
  KEY `FK_r1pno4o57sn1auxbomjueevxi` (`jade_disk_archive_id`),
  KEY `FK_5opbe25ybatqc6gw2p3ys7rbc` (`jade_data_stream_id`),
  CONSTRAINT `FK_5opbe25ybatqc6gw2p3ys7rbc` FOREIGN KEY (`jade_data_stream_id`) REFERENCES `jade_data_stream` (`jade_data_stream_id`),
  CONSTRAINT `FK_r1pno4o57sn1auxbomjueevxi` FOREIGN KEY (`jade_disk_archive_id`) REFERENCES `jade_disk_archive` (`jade_disk_archive_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jade_map_data_stream_to_globus_archive`
--

DROP TABLE IF EXISTS `jade_map_data_stream_to_globus_archive`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jade_map_data_stream_to_globus_archive` (
  `jade_data_stream_id` bigint(20) NOT NULL,
  `jade_globus_archive_id` bigint(20) NOT NULL,
  KEY `FK_lxole11rvu4wr3vb4t6ptiv33` (`jade_globus_archive_id`),
  KEY `FK_gylr0wcby36pxwhy75l83t3ee` (`jade_data_stream_id`),
  CONSTRAINT `FK_gylr0wcby36pxwhy75l83t3ee` FOREIGN KEY (`jade_data_stream_id`) REFERENCES `jade_data_stream` (`jade_data_stream_id`),
  CONSTRAINT `FK_lxole11rvu4wr3vb4t6ptiv33` FOREIGN KEY (`jade_globus_archive_id`) REFERENCES `jade_globus_archive` (`jade_globus_archive_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jade_map_data_stream_to_rudics_archive`
--

DROP TABLE IF EXISTS `jade_map_data_stream_to_rudics_archive`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jade_map_data_stream_to_rudics_archive` (
  `jade_data_stream_id` bigint(20) NOT NULL,
  `jade_rudics_archive_id` bigint(20) NOT NULL,
  KEY `FK_lcbg88v2qnmbetaqf6me15d6b` (`jade_rudics_archive_id`),
  KEY `FK_4l7kskrol0yyyeosv8wpx4vpv` (`jade_data_stream_id`),
  CONSTRAINT `FK_4l7kskrol0yyyeosv8wpx4vpv` FOREIGN KEY (`jade_data_stream_id`) REFERENCES `jade_data_stream` (`jade_data_stream_id`),
  CONSTRAINT `FK_lcbg88v2qnmbetaqf6me15d6b` FOREIGN KEY (`jade_rudics_archive_id`) REFERENCES `jade_rudics_archive` (`jade_rudics_archive_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jade_map_data_stream_to_tdrss_archive`
--

DROP TABLE IF EXISTS `jade_map_data_stream_to_tdrss_archive`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jade_map_data_stream_to_tdrss_archive` (
  `jade_data_stream_id` bigint(20) NOT NULL,
  `jade_tdrss_archive_id` bigint(20) NOT NULL,
  KEY `FK_1od4ub3vh7xyqo1j8isdneis1` (`jade_tdrss_archive_id`),
  KEY `FK_6r2d5rrg45qctfnmgpxxs1f5n` (`jade_data_stream_id`),
  CONSTRAINT `FK_1od4ub3vh7xyqo1j8isdneis1` FOREIGN KEY (`jade_tdrss_archive_id`) REFERENCES `jade_tdrss_archive` (`jade_tdrss_archive_id`),
  CONSTRAINT `FK_6r2d5rrg45qctfnmgpxxs1f5n` FOREIGN KEY (`jade_data_stream_id`) REFERENCES `jade_data_stream` (`jade_data_stream_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jade_map_disk_to_file_pair`
--

DROP TABLE IF EXISTS `jade_map_disk_to_file_pair`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jade_map_disk_to_file_pair` (
  `jade_disk_id` bigint(20) NOT NULL,
  `jade_file_pair_id` bigint(20) NOT NULL,
  `jade_file_pair_order` int(11) NOT NULL,
  PRIMARY KEY (`jade_disk_id`,`jade_file_pair_order`),
  KEY `FK_5ffa8a0kwgyipccwxiw2p32xy` (`jade_file_pair_id`),
  CONSTRAINT `FK_5ffa8a0kwgyipccwxiw2p32xy` FOREIGN KEY (`jade_file_pair_id`) REFERENCES `jade_file_pair` (`jade_file_pair_id`),
  CONSTRAINT `FK_fjqtnjk4hwnlufx4qxnfe2v8l` FOREIGN KEY (`jade_disk_id`) REFERENCES `jade_disk` (`jade_disk_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jade_map_location_to_warehouse_file`
--

DROP TABLE IF EXISTS `jade_map_location_to_warehouse_file`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jade_map_location_to_warehouse_file` (
  `jade_warehouse_file_id` bigint(20) NOT NULL,
  `jade_location_id` bigint(20) NOT NULL,
  `jade_location_order` int(11) NOT NULL,
  PRIMARY KEY (`jade_warehouse_file_id`,`jade_location_order`),
  KEY `FK_bffb2u6pap54km5a2dwi5ocbu` (`jade_location_id`),
  CONSTRAINT `FK_11q98fr2wa2dbegijj370cqvl` FOREIGN KEY (`jade_warehouse_file_id`) REFERENCES `jade_warehouse_file` (`jade_warehouse_file_id`),
  CONSTRAINT `FK_bffb2u6pap54km5a2dwi5ocbu` FOREIGN KEY (`jade_location_id`) REFERENCES `jade_location` (`jade_location_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jade_map_mirror_request_to_warehouse_file`
--

DROP TABLE IF EXISTS `jade_map_mirror_request_to_warehouse_file`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jade_map_mirror_request_to_warehouse_file` (
  `jade_mirror_request_id` bigint(20) NOT NULL,
  `jade_warehouse_file_id` bigint(20) NOT NULL,
  `jade_warehouse_file_order` int(11) NOT NULL,
  PRIMARY KEY (`jade_mirror_request_id`,`jade_warehouse_file_order`),
  KEY `FK_8b70wg8sw431e7opcdgb82a5d` (`jade_warehouse_file_id`),
  CONSTRAINT `FK_8b70wg8sw431e7opcdgb82a5d` FOREIGN KEY (`jade_warehouse_file_id`) REFERENCES `jade_warehouse_file` (`jade_warehouse_file_id`),
  CONSTRAINT `FK_dgbppeefm98295jc5xfbfk7g6` FOREIGN KEY (`jade_mirror_request_id`) REFERENCES `jade_mirror_request` (`jade_mirror_request_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jade_map_picker_request_to_picker_file`
--

DROP TABLE IF EXISTS `jade_map_picker_request_to_picker_file`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jade_map_picker_request_to_picker_file` (
  `jade_picker_request_id` bigint(20) NOT NULL,
  `jade_picker_file_id` bigint(20) NOT NULL,
  `jade_picker_file_order` int(11) NOT NULL,
  PRIMARY KEY (`jade_picker_request_id`,`jade_picker_file_order`),
  KEY `FK_iq65oknlr144by0ftq6gvixsh` (`jade_picker_file_id`),
  CONSTRAINT `FK_ehlsq8rbxl506x3pwbkgomhxd` FOREIGN KEY (`jade_picker_request_id`) REFERENCES `jade_picker_request` (`jade_picker_request_id`),
  CONSTRAINT `FK_iq65oknlr144by0ftq6gvixsh` FOREIGN KEY (`jade_picker_file_id`) REFERENCES `jade_picker_file` (`jade_picker_file_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jade_mirror_request`
--

DROP TABLE IF EXISTS `jade_mirror_request`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jade_mirror_request` (
  `jade_mirror_request_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `date_created` datetime DEFAULT NULL,
  `date_updated` datetime DEFAULT NULL,
  `destination` varchar(255) DEFAULT NULL,
  `destination_endpoint_dir` varchar(255) DEFAULT NULL,
  `destination_endpoint_uuid` varchar(255) DEFAULT NULL,
  `disposition` varchar(255) DEFAULT NULL,
  `original_filename` varchar(255) DEFAULT NULL,
  `final_destination` varchar(255) DEFAULT NULL,
  `final_source` varchar(255) DEFAULT NULL,
  `mirror_spec_json` longtext,
  `mirror_type` varchar(255) DEFAULT NULL,
  `source` varchar(255) DEFAULT NULL,
  `source_endpoint_dir` varchar(255) DEFAULT NULL,
  `source_endpoint_uuid` varchar(255) DEFAULT NULL,
  `uuid` char(36) DEFAULT NULL,
  `version` bigint(20) DEFAULT NULL,
  `destination_location_id` bigint(20) DEFAULT NULL,
  `jade_globus_transfer_id` bigint(20) DEFAULT NULL,
  `source_location_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`jade_mirror_request_id`),
  KEY `FK_raq65c72k02hv5yvn1pmxatae` (`destination_location_id`),
  KEY `FK_5j67b5l8dw9i11s2q8aptwni0` (`jade_globus_transfer_id`),
  KEY `FK_ankm1kifxmt4i60xeq592x8m` (`source_location_id`),
  CONSTRAINT `FK_5j67b5l8dw9i11s2q8aptwni0` FOREIGN KEY (`jade_globus_transfer_id`) REFERENCES `jade_globus_transfer` (`jade_globus_transfer_id`),
  CONSTRAINT `FK_ankm1kifxmt4i60xeq592x8m` FOREIGN KEY (`source_location_id`) REFERENCES `jade_location` (`jade_location_id`),
  CONSTRAINT `FK_raq65c72k02hv5yvn1pmxatae` FOREIGN KEY (`destination_location_id`) REFERENCES `jade_location` (`jade_location_id`)
) ENGINE=InnoDB AUTO_INCREMENT=92 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jade_perf_data`
--

DROP TABLE IF EXISTS `jade_perf_data`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jade_perf_data` (
  `jade_perf_data_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `date_created` datetime DEFAULT NULL,
  `date_updated` datetime DEFAULT NULL,
  `perf_name` varchar(255) DEFAULT NULL,
  `perf_value` varchar(255) DEFAULT NULL,
  `uuid` char(36) DEFAULT NULL,
  `version` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`jade_perf_data_id`),
  UNIQUE KEY `perf_name_index` (`perf_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jade_picker_file`
--

DROP TABLE IF EXISTS `jade_picker_file`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jade_picker_file` (
  `jade_picker_file_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `checksum` varchar(255) DEFAULT NULL,
  `date_created` datetime DEFAULT NULL,
  `date_updated` datetime DEFAULT NULL,
  `file_name` varchar(255) DEFAULT NULL,
  `file_size` bigint(20) DEFAULT NULL,
  `pick_directory` varchar(255) DEFAULT NULL,
  `uuid` char(36) DEFAULT NULL,
  `version` bigint(20) DEFAULT NULL,
  `picked_by_host_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`jade_picker_file_id`),
  KEY `FK_irinim9hc6wf3xmvqbjf0xb0d` (`picked_by_host_id`),
  CONSTRAINT `FK_irinim9hc6wf3xmvqbjf0xb0d` FOREIGN KEY (`picked_by_host_id`) REFERENCES `jade_host` (`jade_host_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1266807 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jade_picker_file_metadata`
--

DROP TABLE IF EXISTS `jade_picker_file_metadata`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jade_picker_file_metadata` (
  `jade_picker_file_metadata_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `date_created` datetime DEFAULT NULL,
  `date_updated` datetime DEFAULT NULL,
  `json_metadata` text,
  `uuid` char(36) DEFAULT NULL,
  `version` bigint(20) DEFAULT NULL,
  `xml_checksum` varchar(255) DEFAULT NULL,
  `xml_metadata` text,
  `jade_picker_file_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`jade_picker_file_metadata_id`),
  KEY `FK_bi2poocfg4s7cl9jqgdg0p5fu` (`jade_picker_file_id`),
  CONSTRAINT `FK_bi2poocfg4s7cl9jqgdg0p5fu` FOREIGN KEY (`jade_picker_file_id`) REFERENCES `jade_picker_file` (`jade_picker_file_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1265731 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jade_picker_request`
--

DROP TABLE IF EXISTS `jade_picker_request`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jade_picker_request` (
  `jade_picker_request_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `bundler_spec_json` longtext,
  `date_created` datetime DEFAULT NULL,
  `date_updated` datetime DEFAULT NULL,
  `original_filename` varchar(255) DEFAULT NULL,
  `file_pair_metadata` bit(1) DEFAULT NULL,
  `picker_spec_json` longtext,
  `spec_version` int(11) DEFAULT NULL,
  `spec_reference_id` varchar(255) DEFAULT NULL,
  `spec_source` varchar(255) DEFAULT NULL,
  `uuid` char(36) DEFAULT NULL,
  `version` bigint(20) DEFAULT NULL,
  `requested_by_host_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`jade_picker_request_id`),
  KEY `FK_7tvhepoetav1emwoc4o3syed6` (`requested_by_host_id`),
  CONSTRAINT `FK_7tvhepoetav1emwoc4o3syed6` FOREIGN KEY (`requested_by_host_id`) REFERENCES `jade_host` (`jade_host_id`)
) ENGINE=InnoDB AUTO_INCREMENT=2535 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jade_rudics_archive`
--

DROP TABLE IF EXISTS `jade_rudics_archive`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jade_rudics_archive` (
  `jade_rudics_archive_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `date_created` datetime DEFAULT NULL,
  `date_updated` datetime DEFAULT NULL,
  `description` varchar(255) DEFAULT NULL,
  `i3ms_uri` varchar(255) DEFAULT NULL,
  `uuid` char(36) DEFAULT NULL,
  `version` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`jade_rudics_archive_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jade_stream_metadata`
--

DROP TABLE IF EXISTS `jade_stream_metadata`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jade_stream_metadata` (
  `jade_stream_metadata_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `dif_category` varchar(255) DEFAULT NULL,
  `dif_data_center_email` varchar(255) DEFAULT NULL,
  `dif_data_center_name` varchar(255) DEFAULT NULL,
  `date_created` datetime DEFAULT NULL,
  `date_updated` datetime DEFAULT NULL,
  `dif_sensor_name` varchar(255) DEFAULT NULL,
  `dif_entry_title` varchar(255) DEFAULT NULL,
  `dif_parameters` varchar(255) DEFAULT NULL,
  `sensor_name` varchar(255) DEFAULT NULL,
  `dif_subcategory` varchar(255) DEFAULT NULL,
  `dif_technical_contact_email` varchar(255) DEFAULT NULL,
  `dif_technical_contact_name` varchar(255) DEFAULT NULL,
  `uuid` char(36) DEFAULT NULL,
  `version` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`jade_stream_metadata_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jade_tdrss_archive`
--

DROP TABLE IF EXISTS `jade_tdrss_archive`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jade_tdrss_archive` (
  `jade_tdrss_archive_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `capacity` bigint(20) DEFAULT NULL,
  `date_created` datetime DEFAULT NULL,
  `date_updated` datetime DEFAULT NULL,
  `description` varchar(255) DEFAULT NULL,
  `sptr_capacity` bigint(20) DEFAULT NULL,
  `sptr_directory` varchar(255) DEFAULT NULL,
  `sptr_host` varchar(255) DEFAULT NULL,
  `uuid` char(36) DEFAULT NULL,
  `version` bigint(20) DEFAULT NULL,
  `jade_credentials_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`jade_tdrss_archive_id`),
  KEY `FK_9k7vnr6xjjfiqpbkatw2va56q` (`jade_credentials_id`),
  CONSTRAINT `FK_9k7vnr6xjjfiqpbkatw2va56q` FOREIGN KEY (`jade_credentials_id`) REFERENCES `jade_credentials` (`jade_credentials_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jade_warehouse_file`
--

DROP TABLE IF EXISTS `jade_warehouse_file`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jade_warehouse_file` (
  `jade_warehouse_file_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `binary_checksum` varchar(255) DEFAULT NULL,
  `date_created` datetime DEFAULT NULL,
  `date_updated` datetime DEFAULT NULL,
  `file_name` varchar(255) DEFAULT NULL,
  `file_path` varchar(255) DEFAULT NULL,
  `file_size` bigint(20) DEFAULT NULL,
  `json_metadata` text,
  `uuid` char(36) DEFAULT NULL,
  `version` bigint(20) DEFAULT NULL,
  `warehouse_path` varchar(255) DEFAULT NULL,
  `xml_checksum` varchar(255) DEFAULT NULL,
  `xml_metadata` text,
  PRIMARY KEY (`jade_warehouse_file_id`),
  KEY `jade_warehouse_file_file_path_idx` (`file_path`)
) ENGINE=InnoDB AUTO_INCREMENT=828789 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Insert the one foreign key that we need to populate in test
--

use jade-lta;
insert into jade_host (
    jade_host_id,
    allow_job_claim,
    allow_job_work,
    allow_open_job_claim,
    date_created,
    date_heartbeat,
    date_updated,
    host_name,
    satellite_capable,
    uuid,
    version
) VALUES (
    2,
    false,
    false,
    false,
    now(),
    now(),
    now(),
    'jade-lta',
    false,
    uuid(),
    0
);

/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2019-10-30 15:19:23
