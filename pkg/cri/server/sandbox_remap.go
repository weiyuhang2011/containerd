package server

import (
	"context"
	"errors"
	"fmt"

	"github.com/containerd/containerd/pkg/cri/util"
	"github.com/containerd/log"
	"github.com/golang/protobuf/proto"

	runtime "k8s.io/cri-api/pkg/apis/runtime/v1"
)

const FAKE_SANDBOX_ANNOTATION = "wyh-fake-sandbox-id"

// return mockSbID to scheduled pod
// create empty sandbox in store for mockpod, the empty sandbox is created from scheduled pod config
func (c *criService) sandboxRemap(ctx context.Context, r *runtime.RunPodSandboxRequest, mockSbID string) (*runtime.RunPodSandboxResponse, error) {
	var err error
	config := r.GetConfig()
	log.G(ctx).WithField("author", "wyh").Debugf("Start RunPodSandbox old sandbox id %v", mockSbID)

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
	mockSb.ID = id
	mockSb.Name = name
	log.G(ctx).WithField("author", "wyh").Debugf("add fake sandbox with mock sandbox config %v", mockSb)
	if err := c.sandboxStore.Add(mockSb); err != nil {
		return nil, fmt.Errorf("failed to add sandbox %+v into store: %w", mockSb, err)
	}

	// update mockpod sandbox config to scheduled pod sandbox config
	config.Annotations[FAKE_SANDBOX_ANNOTATION] = id
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
func (c *criService) containerRemap(ctx context.Context, r *runtime.CreateContainerRequest, oldContainerID string) (*runtime.CreateContainerResponse, error) {
	log.G(ctx).WithField("author", "wyh").Debugf("Start CreateContainer old container id %v", oldContainerID)
	newConfig := r.GetConfig()
	oldCntr, err := c.containerStore.Get(oldContainerID)
	if err != nil {
		return nil, fmt.Errorf("an error occurred when try to find container %q: %w", oldContainerID, err)
	}

	if oldCntr.Status.Get().State() == runtime.ContainerState_CONTAINER_RUNNING {
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
		newSandboxID := oldSb.Config.Annotations[FAKE_SANDBOX_ANNOTATION]
		log.G(ctx).WithField("author", "wyh").Debugf("CreateContainer fake sandbox id: %q", newSandboxID)

		oldCntr.ID = id
		oldCntr.Name = name
		oldCntr.SandboxID = newSandboxID
		oldCntr.Config = proto.Clone(oldCntr.Config).(*runtime.ContainerConfig)

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
	} else {
		log.G(ctx).WithField("author", "wyh").Debugf("CreateContainer return old sandbox id %v but it is not running", oldContainerID)
		return nil, fmt.Errorf("the old container is not running %q: %w", oldContainerID, err)
	}
}
