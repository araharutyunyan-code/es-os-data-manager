package com.datamanager.server.entity;

import com.datamanager.common.model.ClusterConfig;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "cluster_configs")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ClusterConfigEntity {

    @Id
    private String id;

    @Column(nullable = false)
    private String name;

    @Column(nullable = false)
    private String host;

    @Column(nullable = false)
    private int port;

    private String username;

    private String password;

    @Column(name = "use_ssl")
    private boolean useSSL;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private ClusterConfig.ClusterType type;

    private String apiKey;

    @Builder.Default
    @Column(name = "connection_timeout")
    private int connectionTimeout = 30000;

    @Builder.Default
    @Column(name = "socket_timeout")
    private int socketTimeout = 30000;

    public ClusterConfig toClusterConfig() {
        return ClusterConfig.builder()
                .id(id)
                .name(name)
                .host(host)
                .port(port)
                .username(username)
                .password(password)
                .useSSL(useSSL)
                .type(type)
                .apiKey(apiKey)
                .connectionTimeout(connectionTimeout)
                .socketTimeout(socketTimeout)
                .build();
    }

    public static ClusterConfigEntity fromClusterConfig(ClusterConfig config) {
        return ClusterConfigEntity.builder()
                .id(config.getId())
                .name(config.getName())
                .host(config.getHost())
                .port(config.getPort())
                .username(config.getUsername())
                .password(config.getPassword())
                .useSSL(config.isUseSSL())
                .type(config.getType())
                .apiKey(config.getApiKey())
                .connectionTimeout(config.getConnectionTimeout())
                .socketTimeout(config.getSocketTimeout())
                .build();
    }
}
