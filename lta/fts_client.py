# fts_client.py
"""The fts_client module provides a client to interact with FTS."""

import datetime
from datetime import timedelta
import logging
from logging import Logger
from pathlib import Path
from typing import Any, Dict, Optional

from cryptography import x509
from cryptography.x509 import Certificate, CertificateSigningRequest
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.hashes import SHA1
from rest_tools.client import RestClient  # type: ignore

JOB_ID_GENERATOR_STANDARD = 'standard'            # Default algorithm using uuid1
JOB_ID_GENERATOR_DETERMINISTIC = 'deterministic'  # Deterministic algorithm using uuid5 with base_id+vo+sid given by the user

class DecodingRestClient(RestClient):
    """A RestClient with more flexible decoding."""

    def __init__(self, address, token=None, timeout=60.0, retries=10, **kwargs):
        """Create a DecodingRestClient object."""
        super(DecodingRestClient, self).__init__(address, token, timeout, retries, **kwargs)

    def _decode(self, content):
        """Decode from JSON to a Dict if possible, otherwise just return the content."""
        try:
            return super(DecodingRestClient, self)._decode(content)
        except Exception:
            return content

class FTSClient:
    """FTSClient helps to interact with FTS via its RESTful interface."""

    def __init__(self,
                 fts_url: str,
                 timeout: int = 30,
                 retries: int = 3,
                 sslcert: str = None,
                 sslkey: str = None,
                 cacert: str = None,
                 logger: Optional[Logger] = None):
        """Initialize an FTSClient object."""
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger('lta.transfer.FTS')
            self.logger.addHandler(logging.NullHandler())
        self.rest = DecodingRestClient(fts_url, timeout=timeout, retries=retries, sslcert=sslcert, sslkey=sslkey, cacert=cacert)
        self.url = fts_url
        if sslcert:
            self.x509_cert = Path(sslcert).read_text()
        if sslkey:
            self.x509_key = Path(sslkey).read_text()
        if cacert:
            self.x509_cacert = Path(cacert).read_text()

    # TODO: Come back to this to review use within the class
    def delegate(self,
                 lifetime=timedelta(hours=7),
                 force=False,
                 delegate_when_lifetime_lt=timedelta(hours=2)):
        """
        Delegate the credentials.

        Args:
            lifetime: The delegation life time
            force:    If true, credentials will be re-delegated regardless
                      of the remaining life of the previous delegation
            delegate_when_lifetime_lt: If the remaining lifetime on the delegated proxy is less than this interval,
                      do a new delegation

        Returns:
            The delegation ID
        """
        delegator = Delegator(self)
        return delegator.delegate(lifetime, force, delegate_when_lifetime_lt)

    async def get_delegation_id(self) -> str:
        """Obtain our current delegation_id with FTS."""
        result = await self.whoami()
        delegation_id = result["delegation_id"]
        return delegation_id

    async def get_delegation_info(self, delegation_id=None) -> Dict[str, Any]:
        """Obtain information about credential delegation to FTS."""
        if not delegation_id:
            delegation_id = await self.get_delegation_id()
        result = await self.rest.request("GET", f"/delegation/{delegation_id}")
        return result

    async def get_delegation_request(self, delegation_id=None) -> str:
        """Obtain a certificate request to sign for delegation."""
        if not delegation_id:
            delegation_id = await self.get_delegation_id()
        result = await self.rest.request("GET", f"/delegation/{delegation_id}/request")
        print(f"DEBUG: get_delegation_request: {result}")
        return result

    async def get_endpoint_info(self) -> Dict[str, Any]:
        """Obtain information about the FTS endpoint."""
        endpoint_info = await self.rest.request("GET", "/")
        endpoint_info['url'] = self.url
        return endpoint_info

    async def list_jobs(self) -> Dict[str, Any]:
        """Obtain information about our jobs at FTS."""
        result = await self.rest.request("GET", "/jobs")
        return result

    def new_job(self, transfers=None, deletion=None, verify_checksum=False, reuse=None,
                overwrite=False, multihop=False, source_spacetoken=None, spacetoken=None,
                bring_online=None, archive_timeout=None, copy_pin_lifetime=None,
                retry=-1, retry_delay=0, metadata=None, priority=None, strict_copy=False,
                max_time_in_queue=None, timeout=None, id_generator=JOB_ID_GENERATOR_STANDARD,
                sid=None, s3alternate=False, nostreams=1):
        """
        Create a new dictionary representing a job.

        Args:
            transfers:         Initial list of transfers
            deletion:          Delete files
            verify_checksum:   Enable checksum verification: source, destination, both or none
            reuse:             Enable reuse (all transfers are handled by the same process)
            overwrite:         Overwrite the destinations if exist
            multihop:          Treat the transfer as a multihop transfer
            source_spacetoken: Source space token
            spacetoken:        Destination space token
            bring_online:      Bring online timeout
            archive_timeout:   Archive timeout
            copy_pin_lifetime: Pin lifetime
            retry:             Number of retries: <0 is no retries, 0 is server default, >0 is whatever value is passed
            metadata:          Metadata to bind to the job
            priority:          Job priority
            max_time_in_queue: Maximum number
            id_generator:      Job id generator algorithm
            sid:               Specific id given by the client
            s3alternate:       Use S3 alternate url schema
            nostreams:         Number of streams

        Returns:
            An initialized dictionary representing a job
        """
        if transfers is None and deletion is None:
            raise ValueError('Bad request: No transfers or deletion jobs are provided')

        if transfers is None:
            transfers = []

        if isinstance(verify_checksum, str):
            if verify_checksum not in ('source', 'target', 'both', 'none'):
                raise ValueError('Bad request: verify_checksum does not contain a valid value')

        params = {
            "verify_checksum": verify_checksum,
            "reuse": reuse,
            "spacetoken": spacetoken,
            "bring_online": bring_online,
            "archive_timeout": archive_timeout,
            "copy_pin_lifetime": copy_pin_lifetime,
            "job_metadata": metadata,
            "source_spacetoken": source_spacetoken,
            "overwrite": overwrite,
            "multihop": multihop,
            "retry": retry,
            "retry_delay": retry_delay,
            "priority": priority,
            "strict_copy": strict_copy,
            "max_time_in_queue": max_time_in_queue,
            "timeout": timeout,
            "id_generator": id_generator,
            "sid": sid,
            "s3alternate": s3alternate,
            "nostreams": nostreams,
        }
        job = {
            "files": transfers,
            "delete": deletion,
            "params": params,
        }
        return job

    def new_transfer(self,
                     source,
                     destination,
                     checksum='ADLER32',
                     filesize=None,
                     metadata=None,
                     activity=None,
                     selection_strategy='auto'):
        """
        Create a new transfer pair.

        Args:
            source:             Source SURL
            destination:        Destination SURL
            checksum:           Checksum
            filesize:           File size
            metadata:           Metadata to bind to the transfer
            selection_strategy: selection Strategy to implement for multiple replica Jobs

        Returns:
            An initialized transfer
        """
        transfer = {
            "sources": [source],
            "destinations": [destination],
        }
        if checksum:
            transfer['checksum'] = checksum
        if filesize:
            transfer['filesize'] = filesize
        if metadata:
            transfer['metadata'] = metadata
        if activity:
            transfer['activity'] = activity
        if selection_strategy:
            transfer['selection_strategy'] = selection_strategy

        return transfer

    # TODO: Come back to fix this; remove implicit context object (self = context)
    def submit(self,
               context,
               job,
               delegation_lifetime=timedelta(hours=7),
               force_delegation=False,
               delegate_when_lifetime_lt=timedelta(hours=2)):
        """
        Submit a job.

        Args:
            context: fts3.rest.client.context.Context instance
            job:     Dictionary representing the job
            delegation_lifetime: Delegation lifetime
            force_delegation:    Force delegation even if there is a valid proxy
            delegate_when_lifetime_lt: If the remaining lifetime on the delegated proxy is less than this interval,
                      do a new delegation

        Returns:
            The job id
        """
        delegate(context, delegation_lifetime, force_delegation, delegate_when_lifetime_lt)
        submitter = Submitter(context)
        params = job.get('params', {})
        return submitter.submit(
            transfers=job.get('files', None), delete=job.get('delete', None), staging=job.get('staging', None),
            **params
        )

    async def whoami(self) -> Dict[str, Any]:
        """Obtain information about the current authentication context."""
        result = await self.rest.request("GET", "/whoami")
        return result


class Delegator:

    # nid = {'C'                      : m2.NID_countryName,
    #        'SP'                     : m2.NID_stateOrProvinceName,
    #        'ST'                     : m2.NID_stateOrProvinceName,
    #        'stateOrProvinceName'    : m2.NID_stateOrProvinceName,
    #        'L'                      : m2.NID_localityName,
    #        'localityName'           : m2.NID_localityName,
    #        'O'                      : m2.NID_organizationName,
    #        'organizationName'       : m2.NID_organizationName,
    #        'OU'                     : m2.NID_organizationalUnitName,
    #        'organizationUnitName'   : m2.NID_organizationalUnitName,
    #        'CN'                     : m2.NID_commonName,
    #        'commonName'             : m2.NID_commonName,
    #        'Email'                  : m2.NID_pkcs9_emailAddress,
    #        'emailAddress'           : m2.NID_pkcs9_emailAddress,
    #        'serialNumber'           : m2.NID_serialNumber,
    #        'SN'                     : m2.NID_surname,
    #        'surname'                : m2.NID_surname,
    #        'GN'                     : m2.NID_givenName,
    #        'givenName'              : m2.NID_givenName,
    #        'DC'                     : 391
    #        }

    def __init__(self, client):
        """Initialize a Delegator object."""
        self.client = client

    def as_pem(self, cert: Certificate) -> str:
        """Give the PEM encoded version of the provided Certificate."""
        return cert.public_bytes(serialization.Encoding.PEM).decode("utf-8").strip()

    async def _get_delegation_id(self):
        """Obtain delegation_id from whoami."""
        result = await self.client.whoami()
        return result['delegation_id']

    async def _get_remaining_life(self, delegation_id):
        """Determine time until expiry of delegated credentials."""
        r = await self.client.get_delegation_info(delegation_id)
        if r:
            expiration_time = datetime.strptime(r['termination_time'], '%Y-%m-%dT%H:%M:%S')
            return expiration_time - datetime.utcnow()
        return None

    async def _get_proxy_request(self, delegation_id) -> CertificateSigningRequest:
        request_pem = await self.client.get_delegation_request(delegation_id)
        print(f"DEBUG: request_pem: {request_pem}")
        # x509_request = X509.load_request_string(request_pem)
        x509_request = x509.load_pem_x509_csr(request_pem, backend=default_backend())
        # if x509_request.verify(x509_request.get_pubkey()) != 1:
        #     raise ServerError('Error verifying signature on the request:', Err.get_error())
        if not x509_request.is_signature_valid:
            raise ValueError(f"Invalid signature on the request: {x509_request}")
        # Return
        return x509_request

    def _sign_request(self,
                      x509_request: CertificateSigningRequest,
                      lifetime: timedelta) -> Certificate:
        """Sign the CertificateSigningRequest."""
        client_cert = x509.load_pem_x509_certificate(bytes(self.client.x509_cert, encoding="utf-8"), backend=default_backend())
        private_key = serialization.load_pem_private_key(bytes(self.client.x509_key, encoding="utf-8"), password=None, backend=default_backend())

        # not_before = ASN1.ASN1_UTCTIME()
        # not_before.set_datetime(datetime.now(UTC))
        # not_after = ASN1.ASN1_UTCTIME()
        # not_after.set_datetime(datetime.now(UTC) + lifetime)
        not_before = datetime.datetime.now(datetime.timezone.utc)
        not_after = datetime.datetime.now(datetime.timezone.utc) + lifetime

        # proxy_subject = X509.X509_Name()
        # for entry in self.context.x509.get_subject():
        #     ret = m2.x509_name_add_entry(proxy_subject._ptr(), entry._ptr(), -1, 0)
        #     if ret == 0:
        #         raise Exception(
        #             "%s: '%s'" % (m2.err_reason_error_string(m2.err_get_error()), entry)
        #         )

        # proxy = X509.X509()
        builder = x509.CertificateBuilder()
        # proxy.set_serial_number(self.context.x509.get_serial_number())
        builder = builder.serial_number(client_cert.serial_number)
        # proxy.set_version(x509_request.get_version())
        # ???
        # proxy.set_issuer(self.context.x509.get_subject())
        builder = builder.issuer_name(client_cert.subject)
        # proxy.set_pubkey(x509_request.get_pubkey())
        builder = builder.public_key(x509_request.public_key())

        # # Extensions are broken in SL5!!
        # if _m2crypto_extensions_broken():
        #     log.warning("X509v3 extensions disabled!")
        # else:
        #     # X509v3 Basic Constraints
        #     proxy.add_ext(X509.new_extension('basicConstraints', 'CA:FALSE', critical=True))
        #     # X509v3 Key Usage
        #     proxy.add_ext(X509.new_extension('keyUsage', 'Digital Signature, Key Encipherment', critical=True))
        #     #X509v3 Authority Key Identifier
        #     identifier_ext = _workaround_new_extension(
        #         'authorityKeyIdentifier', 'keyid', critical=False, issuer=self.context.x509
        #     )
        #     proxy.add_ext(identifier_ext)

        # any_rfc_proxies = False
        # # FTS-1217 Ignore the user input and select the min proxy lifetime available on the list
        # min_cert_lifetime = self.context.x509_list[0].get_not_after()
        # for cert in self.context.x509_list:
        #     if cert.get_not_after().get_datetime() < min_cert_lifetime.get_datetime():
        #         not_after = cert.get_not_after()
        #         min_cert_lifetime = cert.get_not_after()
        #     try:
        #         cert.get_ext('proxyCertInfo')
        #         any_rfc_proxies = True
        #     except:
        #         pass

        # proxy.set_not_after(not_after)
        builder = builder.not_valid_after(not_after)
        # proxy.set_not_before(not_before)
        builder = builder.not_valid_before(not_before)

        # if any_rfc_proxies:
        #     if _m2crypto_extensions_broken():
        #         raise NotImplementedError("X509v3 extensions are disabled, so RFC proxies can not be generated!")
        #     else:
        #         _add_rfc3820_extensions(proxy)
        #
        # if any_rfc_proxies:
        #     m2.x509_name_set_by_nid(proxy_subject._ptr(), X509.X509_Name.nid['commonName'], str(int(time.time())))
        # else:
        #     m2.x509_name_set_by_nid(proxy_subject._ptr(), X509.X509_Name.nid['commonName'], 'proxy')

        # proxy.set_subject(proxy_subject)
        builder = builder.subject_name(client_cert.subject)
        # proxy.set_version(2)
        # ???
        # proxy.sign(self.context.evp_key, 'sha1')
        proxy = builder.sign(private_key, SHA1(), backend=default_backend())

        return proxy

    def _put_proxy(self, delegation_id, x509_proxy):
        self.context.put('/delegation/' + delegation_id + '/credential', x509_proxy)

    def _full_proxy_chain(self, x509_proxy):
        chain = x509_proxy.as_pem()
        for cert in self.context.x509_list:
            chain += cert.as_pem()
        return chain

    async def delegate(self, lifetime=timedelta(hours=7), force=False, delegate_when_lifetime_lt=timedelta(hours=2)):
        """Delegate X509 credentials to FTS."""
        try:
            delegation_id = await self.client.get_delegation_id()
            print(f"DEBUG: Delegation ID: {delegation_id}")

            remaining_life = await self._get_remaining_life(delegation_id)
            print(f"DEBUG: remaining_life: {remaining_life}")

            # if remaining_life is None:
            #     log.debug("No previous delegation found")
            # elif remaining_life <= timedelta(0):
            #     log.debug("The delegated credentials expired")
            # elif self.context.access_method == 'oauth2' or remaining_life >= delegate_when_lifetime_lt:
            #     if self.context.access_method == 'oauth2' or not force:
            #         log.debug("Not bothering doing the delegation")
            #         return delegation_id
            #     else:
            #         log.debug("Delegation not expired, but this is a forced delegation")
            if remaining_life:
                if (remaining_life >= delegate_when_lifetime_lt) and (not force):
                    return delegation_id

            # Ask for the request
            print("DEBUG: Delegating")
            x509_request = await self._get_proxy_request(delegation_id)
            print(f"DEBUG: x509_request: {x509_request}")

            # Sign request
            print("DEBUG: Signing request")
            x509_proxy = self._sign_request(x509_request, lifetime)
            print(f"DEBUG: x509_proxy: {x509_proxy}")
            raise Exception("This is where we are now...")
            x509_proxy_pem = self._full_proxy_chain(x509_proxy)

            # Send the signed proxy
            self._put_proxy(delegation_id, x509_proxy_pem)

            return delegation_id

        except Exception as e:
            raise Exception(str(e))  # , None, sys.exc_info()[2]
