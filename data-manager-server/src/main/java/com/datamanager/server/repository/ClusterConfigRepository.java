package com.datamanager.server.repository;

import com.datamanager.server.entity.ClusterConfigEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ClusterConfigRepository extends JpaRepository<ClusterConfigEntity, String> {
}
