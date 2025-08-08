"""Configuration management for SageMaker FeatureStore CLI"""

import os
from typing import Optional
import boto3
from botocore.exceptions import NoCredentialsError, ProfileNotFound


class Config:
    def __init__(self, profile: Optional[str] = None, region: Optional[str] = None):
        self.profile = profile or os.environ.get('AWS_PROFILE')
        self.region = region or os.environ.get('AWS_DEFAULT_REGION', 'ap-northeast-2')
        self._session = None
        self._featurestore_runtime = None
        self._sagemaker = None
        self._s3 = None
    
    @property
    def session(self):
        if self._session is None:
            try:
                self._session = boto3.Session(
                    profile_name=self.profile,
                    region_name=self.region
                )
            except (NoCredentialsError, ProfileNotFound) as e:
                raise Exception(f"AWS 자격 증명을 찾을 수 없습니다: {e}")
        return self._session
    
    @property
    def featurestore_runtime(self):
        if self._featurestore_runtime is None:
            self._featurestore_runtime = self.session.client('sagemaker-featurestore-runtime')
        return self._featurestore_runtime
    
    @property
    def sagemaker(self):
        if self._sagemaker is None:
            self._sagemaker = self.session.client('sagemaker')
        return self._sagemaker
    
    @property
    def s3(self):
        if self._s3 is None:
            self._s3 = self.session.client('s3')
        return self._s3