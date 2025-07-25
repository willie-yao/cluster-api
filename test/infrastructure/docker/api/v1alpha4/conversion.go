/*
Copyright 2021 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha4

import (
	"maps"
	"reflect"
	"slices"
	"sort"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiconversion "k8s.io/apimachinery/pkg/conversion"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/conversion"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	clusterv1alpha4 "sigs.k8s.io/cluster-api/internal/api/core/v1alpha4"
	infrav1 "sigs.k8s.io/cluster-api/test/infrastructure/docker/api/v1beta2"
	utilconversion "sigs.k8s.io/cluster-api/util/conversion"
)

func (src *DockerCluster) ConvertTo(dstRaw conversion.Hub) error {
	dst := dstRaw.(*infrav1.DockerCluster)

	if err := Convert_v1alpha4_DockerCluster_To_v1beta2_DockerCluster(src, dst, nil); err != nil {
		return err
	}

	// Manually restore data.
	restored := &infrav1.DockerCluster{}
	ok, err := utilconversion.UnmarshalData(src, restored)
	if err != nil {
		return err
	}

	// Recover intent for bool values converted to *bool.
	initialization := infrav1.DockerClusterInitializationStatus{}
	restoredDockerClusterProvisioned := restored.Status.Initialization.Provisioned
	clusterv1.Convert_bool_To_Pointer_bool(src.Status.Ready, ok, restoredDockerClusterProvisioned, &initialization.Provisioned)
	if !reflect.DeepEqual(initialization, infrav1.DockerClusterInitializationStatus{}) {
		dst.Status.Initialization = initialization
	}

	if ok {
		RestoreDockerClusterSpec(&restored.Spec, &dst.Spec)
		RestoreDockerClusterStatus(&restored.Status, &dst.Status)
	}

	return nil
}

func RestoreDockerClusterSpec(restored *infrav1.DockerClusterSpec, dst *infrav1.DockerClusterSpec) {
	if restored.LoadBalancer.CustomHAProxyConfigTemplateRef != nil {
		dst.LoadBalancer.CustomHAProxyConfigTemplateRef = restored.LoadBalancer.CustomHAProxyConfigTemplateRef
	}
}

func RestoreDockerClusterStatus(restored *infrav1.DockerClusterStatus, dst *infrav1.DockerClusterStatus) {
	// Restore fields added in v1beta2.
	dst.Conditions = restored.Conditions
}

func (dst *DockerCluster) ConvertFrom(srcRaw conversion.Hub) error {
	src := srcRaw.(*infrav1.DockerCluster)

	if err := Convert_v1beta2_DockerCluster_To_v1alpha4_DockerCluster(src, dst, nil); err != nil {
		return err
	}

	return utilconversion.MarshalData(src, dst)
}

func (src *DockerClusterTemplate) ConvertTo(dstRaw conversion.Hub) error {
	dst := dstRaw.(*infrav1.DockerClusterTemplate)

	if err := Convert_v1alpha4_DockerClusterTemplate_To_v1beta2_DockerClusterTemplate(src, dst, nil); err != nil {
		return err
	}

	// Manually restore data.
	restored := &infrav1.DockerClusterTemplate{}
	ok, err := utilconversion.UnmarshalData(src, restored)
	if err != nil {
		return err
	}

	if ok {
		RestoreDockerClusterTemplateSpec(&restored.Spec, &dst.Spec)
	}

	return nil
}

func RestoreDockerClusterTemplateSpec(restored *infrav1.DockerClusterTemplateSpec, dst *infrav1.DockerClusterTemplateSpec) {
	// Restore fields added in v1beta2.
	dst.Template.ObjectMeta = restored.Template.ObjectMeta

	if restored.Template.Spec.LoadBalancer.CustomHAProxyConfigTemplateRef != nil {
		dst.Template.Spec.LoadBalancer.CustomHAProxyConfigTemplateRef = restored.Template.Spec.LoadBalancer.CustomHAProxyConfigTemplateRef
	}
}

func (dst *DockerClusterTemplate) ConvertFrom(srcRaw conversion.Hub) error {
	src := srcRaw.(*infrav1.DockerClusterTemplate)

	if err := Convert_v1beta2_DockerClusterTemplate_To_v1alpha4_DockerClusterTemplate(src, dst, nil); err != nil {
		return err
	}

	return utilconversion.MarshalData(src, dst)
}

func (src *DockerMachine) ConvertTo(dstRaw conversion.Hub) error {
	dst := dstRaw.(*infrav1.DockerMachine)

	if err := Convert_v1alpha4_DockerMachine_To_v1beta2_DockerMachine(src, dst, nil); err != nil {
		return err
	}

	// Manually restore data.
	restored := &infrav1.DockerMachine{}
	ok, err := utilconversion.UnmarshalData(src, restored)
	if err != nil {
		return err
	}

	// Recover intent for bool values converted to *bool.
	initialization := infrav1.DockerMachineInitializationStatus{}
	restoredDockerMachineProvisioned := restored.Status.Initialization.Provisioned
	clusterv1.Convert_bool_To_Pointer_bool(src.Status.Ready, ok, restoredDockerMachineProvisioned, &initialization.Provisioned)
	if !reflect.DeepEqual(initialization, infrav1.DockerMachineInitializationStatus{}) {
		dst.Status.Initialization = initialization
	}

	if ok {
		RestoreDockerMachineSpec(&restored.Spec, &dst.Spec)
		RestoreDockerMachineStatus(&restored.Status, &dst.Status)
	}

	return nil
}

func RestoreDockerMachineSpec(restored *infrav1.DockerMachineSpec, dst *infrav1.DockerMachineSpec) {
	// Restore fields added in v1beta2.
	if restored.BootstrapTimeout != nil {
		dst.BootstrapTimeout = restored.BootstrapTimeout
	}
}

func RestoreDockerMachineStatus(restored *infrav1.DockerMachineStatus, dst *infrav1.DockerMachineStatus) {
	// Restore fields added in v1beta2.
	dst.Conditions = restored.Conditions
}

func (dst *DockerMachine) ConvertFrom(srcRaw conversion.Hub) error {
	src := srcRaw.(*infrav1.DockerMachine)

	if err := Convert_v1beta2_DockerMachine_To_v1alpha4_DockerMachine(src, dst, nil); err != nil {
		return err
	}

	if dst.Spec.ProviderID != nil && *dst.Spec.ProviderID == "" {
		dst.Spec.ProviderID = nil
	}

	return utilconversion.MarshalData(src, dst)
}

func (src *DockerMachineTemplate) ConvertTo(dstRaw conversion.Hub) error {
	dst := dstRaw.(*infrav1.DockerMachineTemplate)

	if err := Convert_v1alpha4_DockerMachineTemplate_To_v1beta2_DockerMachineTemplate(src, dst, nil); err != nil {
		return err
	}

	// Manually restore data.
	restored := &infrav1.DockerMachineTemplate{}
	ok, err := utilconversion.UnmarshalData(src, restored)
	if err != nil {
		return err
	}

	if ok {
		RestoreDockerMachineTemplateSpec(&restored.Spec, &dst.Spec)
	}

	return nil
}

func RestoreDockerMachineTemplateSpec(restored *infrav1.DockerMachineTemplateSpec, dst *infrav1.DockerMachineTemplateSpec) {
	// Restore fields added in v1beta2.
	dst.Template.ObjectMeta = restored.Template.ObjectMeta

	if restored.Template.Spec.BootstrapTimeout != nil {
		dst.Template.Spec.BootstrapTimeout = restored.Template.Spec.BootstrapTimeout
	}
}

func (dst *DockerMachineTemplate) ConvertFrom(srcRaw conversion.Hub) error {
	src := srcRaw.(*infrav1.DockerMachineTemplate)

	if err := Convert_v1beta2_DockerMachineTemplate_To_v1alpha4_DockerMachineTemplate(src, dst, nil); err != nil {
		return err
	}

	if dst.Spec.Template.Spec.ProviderID != nil && *dst.Spec.Template.Spec.ProviderID == "" {
		dst.Spec.Template.Spec.ProviderID = nil
	}

	return utilconversion.MarshalData(src, dst)
}

func Convert_v1beta2_DockerClusterTemplateResource_To_v1alpha4_DockerClusterTemplateResource(in *infrav1.DockerClusterTemplateResource, out *DockerClusterTemplateResource, s apiconversion.Scope) error {
	// NOTE: custom conversion func is required because spec.template.metadata has been added in v1beta1.
	return autoConvert_v1beta2_DockerClusterTemplateResource_To_v1alpha4_DockerClusterTemplateResource(in, out, s)
}

func Convert_v1beta2_DockerMachineTemplateResource_To_v1alpha4_DockerMachineTemplateResource(in *infrav1.DockerMachineTemplateResource, out *DockerMachineTemplateResource, s apiconversion.Scope) error {
	// NOTE: custom conversion func is required because spec.template.metadata has been added in v1beta1.
	return autoConvert_v1beta2_DockerMachineTemplateResource_To_v1alpha4_DockerMachineTemplateResource(in, out, s)
}

func Convert_v1beta2_DockerLoadBalancer_To_v1alpha4_DockerLoadBalancer(in *infrav1.DockerLoadBalancer, out *DockerLoadBalancer, s apiconversion.Scope) error {
	return autoConvert_v1beta2_DockerLoadBalancer_To_v1alpha4_DockerLoadBalancer(in, out, s)
}

func Convert_v1beta2_DockerMachineSpec_To_v1alpha4_DockerMachineSpec(in *infrav1.DockerMachineSpec, out *DockerMachineSpec, s apiconversion.Scope) error {
	return autoConvert_v1beta2_DockerMachineSpec_To_v1alpha4_DockerMachineSpec(in, out, s)
}

func Convert_v1beta2_DockerClusterStatus_To_v1alpha4_DockerClusterStatus(in *infrav1.DockerClusterStatus, out *DockerClusterStatus, s apiconversion.Scope) error {
	if err := autoConvert_v1beta2_DockerClusterStatus_To_v1alpha4_DockerClusterStatus(in, out, s); err != nil {
		return err
	}

	// Reset conditions from autogenerated conversions
	// NOTE: v1beta2 conditions should not be automatically be converted into v1alpha4 conditions.
	out.Conditions = nil
	if in.Deprecated != nil && in.Deprecated.V1Beta1 != nil && in.Deprecated.V1Beta1.Conditions != nil {
		clusterv1alpha4.Convert_v1beta2_Deprecated_V1Beta1_Conditions_To_v1alpha4_Conditions(&in.Deprecated.V1Beta1.Conditions, &out.Conditions)
	}

	out.Ready = ptr.Deref(in.Initialization.Provisioned, false)

	// Move FailureDomains
	if in.FailureDomains != nil {
		out.FailureDomains = clusterv1alpha4.FailureDomains{}
		for _, fd := range in.FailureDomains {
			out.FailureDomains[fd.Name] = clusterv1alpha4.FailureDomainSpec{
				ControlPlane: ptr.Deref(fd.ControlPlane, false),
				Attributes:   fd.Attributes,
			}
		}
	}

	return nil
}

func Convert_v1beta2_DockerMachineStatus_To_v1alpha4_DockerMachineStatus(in *infrav1.DockerMachineStatus, out *DockerMachineStatus, s apiconversion.Scope) error {
	if err := autoConvert_v1beta2_DockerMachineStatus_To_v1alpha4_DockerMachineStatus(in, out, s); err != nil {
		return err
	}

	// Reset conditions from autogenerated conversions
	// NOTE: v1beta2 conditions should not be automatically be converted into v1alpha4 conditions.
	out.Conditions = nil
	if in.Deprecated != nil && in.Deprecated.V1Beta1 != nil && in.Deprecated.V1Beta1.Conditions != nil {
		clusterv1alpha4.Convert_v1beta2_Deprecated_V1Beta1_Conditions_To_v1alpha4_Conditions(&in.Deprecated.V1Beta1.Conditions, &out.Conditions)
	}

	out.Ready = ptr.Deref(in.Initialization.Provisioned, false)

	return nil
}

func Convert_v1beta2_DockerCluster_To_v1alpha4_DockerCluster(in *infrav1.DockerCluster, out *DockerCluster, s apiconversion.Scope) error {
	return autoConvert_v1beta2_DockerCluster_To_v1alpha4_DockerCluster(in, out, s)
}

func Convert_v1beta2_DockerClusterTemplate_To_v1alpha4_DockerClusterTemplate(in *infrav1.DockerClusterTemplate, out *DockerClusterTemplate, s apiconversion.Scope) error {
	return autoConvert_v1beta2_DockerClusterTemplate_To_v1alpha4_DockerClusterTemplate(in, out, s)
}

func Convert_v1beta2_DockerMachine_To_v1alpha4_DockerMachine(in *infrav1.DockerMachine, out *DockerMachine, s apiconversion.Scope) error {
	return autoConvert_v1beta2_DockerMachine_To_v1alpha4_DockerMachine(in, out, s)
}

func Convert_v1beta2_DockerMachineTemplate_To_v1alpha4_DockerMachineTemplate(in *infrav1.DockerMachineTemplate, out *DockerMachineTemplate, s apiconversion.Scope) error {
	return autoConvert_v1beta2_DockerMachineTemplate_To_v1alpha4_DockerMachineTemplate(in, out, s)
}

// Implement local conversion func because conversion-gen is not aware of conversion func in other packages (see https://github.com/kubernetes/code-generator/issues/94)

func Convert_v1alpha4_Condition_To_v1_Condition(in *clusterv1alpha4.Condition, out *metav1.Condition, s apiconversion.Scope) error {
	return clusterv1alpha4.Convert_v1alpha4_Condition_To_v1_Condition(in, out, s)
}

func Convert_v1_Condition_To_v1alpha4_Condition(in *metav1.Condition, out *clusterv1alpha4.Condition, s apiconversion.Scope) error {
	return clusterv1alpha4.Convert_v1_Condition_To_v1alpha4_Condition(in, out, s)
}

func Convert_v1alpha4_DockerMachineStatus_To_v1beta2_DockerMachineStatus(in *DockerMachineStatus, out *infrav1.DockerMachineStatus, s apiconversion.Scope) error {
	if err := autoConvert_v1alpha4_DockerMachineStatus_To_v1beta2_DockerMachineStatus(in, out, s); err != nil {
		return err
	}

	// Reset conditions from autogenerated conversions
	// NOTE: v1alpha4 conditions should not be automatically be converted into v1beta2 conditions.
	out.Conditions = nil
	if in.Conditions != nil {
		out.Deprecated = &infrav1.DockerMachineDeprecatedStatus{}
		out.Deprecated.V1Beta1 = &infrav1.DockerMachineV1Beta1DeprecatedStatus{}
		clusterv1alpha4.Convert_v1alpha4_Conditions_To_v1beta2_Deprecated_V1Beta1_Conditions(&in.Conditions, &out.Deprecated.V1Beta1.Conditions)
	}

	return nil
}

func Convert_v1alpha4_DockerClusterStatus_To_v1beta2_DockerClusterStatus(in *DockerClusterStatus, out *infrav1.DockerClusterStatus, s apiconversion.Scope) error {
	// NOTE: custom conversion func is required because status.conditions has been added in v1beta2.
	if err := autoConvert_v1alpha4_DockerClusterStatus_To_v1beta2_DockerClusterStatus(in, out, s); err != nil {
		return err
	}

	// Reset conditions from autogenerated conversions
	// NOTE: v1alpha4 conditions should not be automatically be converted into v1beta2 conditions.
	out.Conditions = nil
	if in.Conditions != nil {
		out.Deprecated = &infrav1.DockerClusterDeprecatedStatus{}
		out.Deprecated.V1Beta1 = &infrav1.DockerClusterV1Beta1DeprecatedStatus{}
		clusterv1alpha4.Convert_v1alpha4_Conditions_To_v1beta2_Deprecated_V1Beta1_Conditions(&in.Conditions, &out.Deprecated.V1Beta1.Conditions)
	}

	// Move FailureDomains
	if in.FailureDomains != nil {
		out.FailureDomains = []clusterv1.FailureDomain{}
		domainNames := slices.Collect(maps.Keys(in.FailureDomains))
		sort.Strings(domainNames)
		for _, name := range domainNames {
			domain := in.FailureDomains[name]
			out.FailureDomains = append(out.FailureDomains, clusterv1.FailureDomain{
				Name:         name,
				ControlPlane: ptr.To(domain.ControlPlane),
				Attributes:   domain.Attributes,
			})
		}
	}

	return nil
}

func Convert_v1alpha4_DockerClusterSpec_To_v1beta2_DockerClusterSpec(in *DockerClusterSpec, out *infrav1.DockerClusterSpec, s apiconversion.Scope) error {
	if err := autoConvert_v1alpha4_DockerClusterSpec_To_v1beta2_DockerClusterSpec(in, out, s); err != nil {
		return err
	}

	// Move FailureDomains
	if in.FailureDomains != nil {
		out.FailureDomains = []clusterv1.FailureDomain{}
		domainNames := slices.Collect(maps.Keys(in.FailureDomains))
		sort.Strings(domainNames)
		for _, name := range domainNames {
			domain := in.FailureDomains[name]
			out.FailureDomains = append(out.FailureDomains, clusterv1.FailureDomain{
				Name:         name,
				ControlPlane: ptr.To(domain.ControlPlane),
				Attributes:   domain.Attributes,
			})
		}
	}

	return nil
}

func Convert_v1beta2_DockerClusterSpec_To_v1alpha4_DockerClusterSpec(in *infrav1.DockerClusterSpec, out *DockerClusterSpec, s apiconversion.Scope) error {
	if err := autoConvert_v1beta2_DockerClusterSpec_To_v1alpha4_DockerClusterSpec(in, out, s); err != nil {
		return err
	}

	// Move FailureDomains
	if in.FailureDomains != nil {
		out.FailureDomains = clusterv1alpha4.FailureDomains{}
		for _, fd := range in.FailureDomains {
			out.FailureDomains[fd.Name] = clusterv1alpha4.FailureDomainSpec{
				ControlPlane: ptr.Deref(fd.ControlPlane, false),
				Attributes:   fd.Attributes,
			}
		}
	}

	return nil
}
