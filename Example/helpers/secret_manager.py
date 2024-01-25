"""modules to import"""
import os
from google.cloud import secretmanager

class GcpSecretManager:
    """Used to create secret and read secrets in the current project
    @param secret_id
    @param payload
    """

    def __init__(self, secret_id = None, payload = None) -> None:
        self.secret_id = secret_id
        self.project_id = os.getenv('PROJECT_ID')
        self.gcp_client = secretmanager.SecretManagerServiceClient()


    def create_secret(self,payload):
        """create secret"""
        parent = f'projects/{self.project_id}'

        secret = self.gcp_client.create_secret(
            request={
                "parent": parent,
                "secret_id": self.secret_id,
                "secret": {"replication": {"automatic": {}}},
            }
        )

        self.gcp_client.add_secret_version(
            request={"parent": secret.name, "payload": {"data" : payload}}
        )

    def get_secret(self, secret_name):
        """
        Args:
            secert_name (string): string

        Returns:
            string: most recent version of the secret
        """

        secret_name = f"projects/{self.project_id}/secrets/{secret_name}/versions/latest"

        response = self.gcp_client.access_secret_version(name=secret_name)
        return response.payload.data.decode("UTF-8")


    def update_secret(self, secret_name, payload):
        """update secret"""
        parent = self.gcp_client.secret_path(self.project_id, secret_name)

        payload = payload.encode("UTF-8")

        response = self.gcp_client.add_secret_version(request={
            "parent" : parent
            , "payload" : {
                "data": payload
            }
            }
        )

        print(f"Added secret version: {response.name}")