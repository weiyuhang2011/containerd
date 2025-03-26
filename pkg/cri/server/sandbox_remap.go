package server

import (
	"context"
	"errors"
	"fmt"

	"github.com/containerd/containerd/pkg/cri/store/container"
	"github.com/containerd/containerd/pkg/cri/store/sandbox"
	"github.com/containerd/containerd/pkg/cri/util"
	"github.com/containerd/log"
	"github.com/golang/protobuf/proto"

	runtime "k8s.io/cri-api/pkg/apis/runtime/v1"
)

const (
	SANDBOX_HANDFROM_ANNOTATION   = "sandboxid.openeuler.org/handed-from"
	SANDBOX_REMAP_ANNOTATION      = "sandboxid.openeuler.org"
	CONTAINER_REMAP_ANNOTATION    = "containerid.openeuler.org"
	SANDBOX_HANDEDTO_ANNOTATION   = "sandboxid.openeuler.org/handed-over-to"
	CONTAINER_HANDEDTO_ANNOTATION = "containerid.openeuler.org/handed-over-to"
)

// Pod:        old-pod                                                  new-pod
// Sandbox:    old-sandbox                                              nil
// Final:      fake-sandbox(old-sandbox with new ID and name)           old-sandbox(update config)
// 1. get new sandbox config
// 2. generate new sandbox ID and name
// 3. create fake-sandbox with new ID, name and old config
// 4. update old-sandbox with new-pod config
// create empty sandbox in store for mockpod, the empty sandbox is created from scheduled pod config
func (c *criService) sandboxRemap(ctx context.Context, r *runtime.RunPodSandboxRequest, mockSbID string) (*runtime.RunPodSandboxResponse, error) {
	fmt.Println("RemapSandbox wyh server")
	var err error
	config := r.GetConfig()
	log.G(ctx).WithField("author", "wyh").Debugf("Start RunPodSandbox old sandbox id %v", mockSbID)

	// create fake sandbox
	id := util.GenerateID()
	metadata := config.GetMetadata()
	if metadata == nil {
		return nil, errors.New("sandbox config must include metadata")
	}
	name := makeSandboxName(metadata)
	if err := c.sandboxNameIndex.Reserve(name, id); err != nil {
		return nil, fmt.Errorf("failed to reserve sandbox name %q: %w", name, err)
	}
	defer func() {
		if err != nil {
			c.sandboxNameIndex.ReleaseByName(name)
		}
	}()
	mockSb, err := c.sandboxStore.Get(mockSbID)
	if err != nil {
		return nil, fmt.Errorf("failed to add sandbox %s metadata config %+v into store: %w", mockSbID, config, err)
	}
	// replace mock sandbox ID and name to fake sandbox, but keep the other fields
	// so that the old pod can have a fake sandbox
	mockSb.ID = id
	mockSb.Name = name
	mockSb.Config.Annotations[SANDBOX_HANDEDTO_ANNOTATION] = mockSbID
	mockSb.Status = sandbox.StoreStatus(mockSb.Status.Get())
	log.G(ctx).WithField("author", "wyh").Debugf("add fake sandbox with mock sandbox config %v", mockSb)
	if err := c.sandboxStore.Add(mockSb); err != nil {
		return nil, fmt.Errorf("failed to add sandbox %+v into store: %w", mockSb, err)
	}

	// update mock sandbox annotation to fake sandbox ID
	config.Annotations[SANDBOX_HANDFROM_ANNOTATION] = id
	if err := c.sandboxStore.UpdateSandboxMetaConfig(mockSbID, config); err != nil {
		return nil, fmt.Errorf("failed to add sandbox %s metadata config %+v into store: %w", mockSbID, config, err)
	}
	log.G(ctx).WithField("author", "wyh").Debugf("update mock sandbox %v with config %v", mockSbID, config)
	c.generateAndSendContainerEvent(ctx, mockSbID, mockSbID, runtime.ContainerEventType_CONTAINER_CREATED_EVENT)

	log.G(ctx).WithField("author", "wyh").Debugf("End RunPodSandbox old sandbox id %v", mockSbID)
	return &runtime.RunPodSandboxResponse{PodSandboxId: mockSbID}, nil
}

// if mock container is running, update mock container label and annotation to scheduled container
// create scheduled container in store for mock container, the scheduled container is created from mock container config
// 1. get old container, return error if the old container is not running
// 2. generate new container ID and name
// 3. get fake sandbox ID from old-sandbox annotation
// 4. create fake container with new ID, name, fake-sandboxid and old config
// 5. update old container with new-container label and annotation
func (c *criService) containerRemap(ctx context.Context, r *runtime.CreateContainerRequest, oldContainerID string) (_ *runtime.CreateContainerResponse, retErr error) {
	log.G(ctx).WithField("author", "wyh").Debugf("Start CreateContainer old container id %v", oldContainerID)
	newConfig := r.GetConfig()
	oldCntr, err := c.containerStore.Get(oldContainerID)
	if err != nil {
		return nil, fmt.Errorf("an error occurred when try to find container %q: %w", oldContainerID, err)
	}

	if oldCntr.Status.Get().State() != runtime.ContainerState_CONTAINER_RUNNING {
		log.G(ctx).WithField("author", "wyh").Debugf("CreateContainer return old sandbox id %v but it is not running", oldContainerID)
		return nil, fmt.Errorf("the old container is not running %q: %w", oldContainerID, err)
	}

	log.G(ctx).WithField("author", "wyh").Debugf("CreateContainer mock container %v is running", oldContainerID)

	id := util.GenerateID()
	newSandboxConfig := r.GetSandboxConfig()
	newMetadata := newConfig.GetMetadata()
	if newMetadata == nil {
		return nil, errors.New("container config must include metadata")
	}
	name := makeContainerName(newMetadata, newSandboxConfig.GetMetadata())
	log.G(ctx).Debugf("Generated id %q for container %q", id, name)
	if err = c.containerNameIndex.Reserve(name, id); err != nil {
		return nil, fmt.Errorf("failed to reserve container name %q: %w", name, err)
	}
	defer func() {
		if err != nil {
			c.containerNameIndex.ReleaseByName(name)
		}
	}()
	oldSb, err := c.sandboxStore.Get(oldCntr.SandboxID)
	if err != nil {
		return nil, fmt.Errorf("failed to get sandbox %q: %w", oldCntr.SandboxID, err)
	}
	log.G(ctx).WithField("author", "wyh").Debugf("CreateContainer mock sandbox %v", oldSb)
	newSandboxID := oldSb.Config.Annotations[SANDBOX_HANDFROM_ANNOTATION]
	log.G(ctx).WithField("author", "wyh").Debugf("CreateContainer fake sandbox id: %q", newSandboxID)

	oldCntr.ID = id
	oldCntr.Name = name
	oldCntr.SandboxID = newSandboxID
	oldCntr.Config = proto.Clone(oldCntr.Config).(*runtime.ContainerConfig)
	oldCntr.Config.Annotations[CONTAINER_HANDEDTO_ANNOTATION] = oldContainerID
	oldCntr.IO = nil

	containerRootDir := c.getContainerRootDir(id)
	if err = c.os.MkdirAll(containerRootDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create container root directory %q: %w",
			containerRootDir, err)
	}
	defer func() {
		if retErr != nil {
			// Cleanup the container root directory.
			if err = c.os.RemoveAll(containerRootDir); err != nil {
				log.G(ctx).WithError(err).Errorf("Failed to remove container root directory %q",
					containerRootDir)
			}
		}
	}()
	oldCntr.Status, err = container.StoreStatus(containerRootDir, id, oldCntr.Status.Get())
	if err != nil {
		return nil, fmt.Errorf("failed to create container status: %w", err)
	}

	log.G(ctx).WithField("author", "wyh").Debugf("CreateContainer create fake container %v", oldCntr)
	if err := c.containerStore.Add(oldCntr); err != nil {
		return nil, fmt.Errorf("failed to add container %q into store: %w", id, err)
	}
	log.G(ctx).WithField("author", "wyh").Debugf("CreateContainer created fake container")

	if err := c.containerStore.UpdateLabelsAnotations(oldContainerID, newConfig.Labels, newConfig.Annotations); err != nil {
		return nil, fmt.Errorf("failed to update container %q labels %v and annotation %v into store: %w", oldContainerID, newConfig.Labels, newConfig.Annotations, err)
	}
	log.G(ctx).WithField("author", "wyh").Debugf("CreateContainer update mock container %v labels %v and annotation %v", oldContainerID, newConfig.Labels, newConfig.Annotations)
	c.generateAndSendContainerEvent(ctx, oldContainerID, r.GetPodSandboxId(), runtime.ContainerEventType_CONTAINER_CREATED_EVENT)
	log.G(ctx).WithField("author", "wyh").Debugf("End CreateContainer mock container id %v", oldContainerID)
	return &runtime.CreateContainerResponse{ContainerId: oldContainerID}, nil
}
