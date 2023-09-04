NEMESIS_REQUIRED_ADDITIONAL_CONFIGS = {
    "StopStartInterfacesNetworkMonkey": ["configurations/network_config/two_interfaces.yaml"],
    "RandomInterruptionNetworkMonkey": ["configurations/network_config/two_interfaces.yaml"],
    "BlockNetworkMonkey": ["configurations/network_config/two_interfaces.yaml"],
    "SlaSevenSlWithMaxSharesDuringLoad": ["configurations/nemesis/additional_configs/sla_config.yaml"],
    "SlaReplaceUsingDropDuringLoad": ["configurations/nemesis/additional_configs/sla_config.yaml"],
    "SlaReplaceUsingDetachDuringLoad": ["configurations/nemesis/additional_configs/sla_config.yaml"],
    "SlaMaximumAllowedSlsWithMaxSharesDuringLoad": ["configurations/nemesis/additional_configs/sla_config.yaml"],
    "SlaIncreaseSharesDuringLoad": ["configurations/nemesis/additional_configs/sla_config.yaml"],
    "SlaIncreaseSharesByAttachAnotherSlDuringLoad": ["configurations/nemesis/additional_configs/sla_config.yaml"],
    "SlaDecreaseSharesDuringLoad": ["configurations/nemesis/additional_configs/sla_config.yaml"],
    "PauseLdapNemesis": ["configurations/ldap-authorization.yaml"],
    "ToggleLdapConfiguration": ["configurations/ldap-authorization.yaml"],
}
