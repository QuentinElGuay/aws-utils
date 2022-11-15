import logging

import boto3


def select_aws_profile(profiles: list[str]) -> str:
    """Select an AWS profil from the locally configured ones..

    Args:
        profiles (list[str]): List of locally configured AWS CLI.

    Returns:
        str: the selected profile.
    """
    
    if not len(profiles):
        return None

    if len(profiles) == 1:
        return profiles[0]

    profile = None
    while not profile:
        prompt_profiles = [
            f'{i} - {v}' for i, v in enumerate(profiles)
        ]
        prompt_profiles.append(
            'Select the number of the profile to use (between 0 and ' \
            f'{len(profiles)-1}): '
        )
        input_value = input('\n'.join(prompt_profiles))

        try:
            idx = int(input_value)
            profile = profiles(idx)
        except:
            print(
                'The inserted value must be an integer between 0 and '\
                f'{len(profiles)}.'
            )
    
    return profile


def get_token(mfa_serial: str) -> dict:
    """Get a session token via AWS STS.

    Args:
        mfa_serial (str): AWS serial number.

    Returns:
        dict: token da sessão
    """

    client = boto3.client('sts')
    mfa_token = input('Input MFA token: ')

    return client.get_session_token(
        SerialNumber=mfa_serial,
        TokenCode=mfa_token
    )


def connect(session:boto3.session.Session) -> boto3.session.Session:
    """Connect to AWS  using Multi Factor Authentication.

    Args:
        session (boto3.session.Session): basic AWS session.

    Raises:
        RuntimeError: _description_
        RuntimeError: _description_

    Returns:
        boto3.session.Session: connected AWS session.
    """
    logging.info('Activating Multi Factor Authentication session.')

    config = session._session.full_config

    profiles = config['profiles']
    if not profiles:
        raise RuntimeError(
            'It is required to install and configure the AWS CLI to be able ' \
            'to use the MFA connection mode.'
        )

    profiles_keys = list(profiles.keys())
    profile_name = select_aws_profile(profiles_keys)
    logging.info('Perfil [%s] selecionado.', profile_name)
    
    profile = profiles[profile_name]
    if 'mfa_serial' not in profile:
        raise RuntimeError(
            'It is required to define the `mfa_serial` value in the AWS CLI ' \
            'configuration.'
        )

    session_token = get_token(profile)
    session = boto3.session.Session(
        aws_access_key_id=session_token['Credentials']['AccessKeyId'],
        aws_secret_access_key=session_token['Credentials']['SecretAccessKey'],
        aws_session_token=session_token['Credentials']['SessionToken'],
    )

    logging.info('Opened AWS session.')
    return session
