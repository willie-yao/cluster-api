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

package scope

import "k8s.io/apimachinery/pkg/util/sets"

// UpgradeTracker is a helper to capture the upgrade status and make upgrade decisions.
type UpgradeTracker struct {
	ControlPlane       ControlPlaneUpgradeTracker
	MachineDeployments MachineDeploymentUpgradeTracker
	MachinePools       MachinePoolUpgradeTracker
}

// ControlPlaneUpgradeTracker holds the current upgrade status of the Control Plane.
type ControlPlaneUpgradeTracker struct {
	// IsPendingUpgrade is true if the Control Plane version needs to be updated. False otherwise.
	// If IsPendingUpgrade is true it also means the Control Plane is not going to pick up the new version
	// in the current reconcile loop.
	// Example cases when IsPendingUpgrade is set to true:
	// - Upgrade is blocked by BeforeClusterUpgrade hook
	// - Upgrade is blocked because the current ControlPlane is not stable (provisioning OR scaling OR upgrading)
	// - Upgrade is blocked because any of the current MachineDeployments are upgrading.
	IsPendingUpgrade bool

	// IsProvisioning is true if the current Control Plane is being provisioned for the first time. False otherwise.
	IsProvisioning bool

	// IsUpgrading is true if the Control Plane is in the middle of an upgrade.
	// Note: Refer to Control Plane contract for definition of upgrading.
	// IsUpgrading is set to true if the current ControlPlane (ControlPlane at the beginning of the reconcile)
	// is upgrading.
	// Note: IsUpgrading only represents the current ControlPlane state. If the Control Plane is about to pick up the
	// version in the reconcile loop IsUpgrading will not be true, because the current ControlPlane is not upgrading,
	// the desired ControlPlane is.
	// Also look at: IsStartingUpgrade.
	IsUpgrading bool

	// IsStartingUpgrade is true if the Control Plane is picking up the new version in the current reconcile loop.
	// If IsStartingUpgrade is true it implies that the desired Control Plane version and the current Control Plane
	// versions are different.
	IsStartingUpgrade bool

	// IsScaling is true if the current Control Plane is scaling. False otherwise.
	// IsScaling only represents the state of the current Control Plane. IsScaling does not represent the state
	// of the desired Control Plane.
	// Example:
	// - IsScaling will be true if the current ControlPlane is scaling.
	// - IsScaling will not be true if the current Control Plane is stable and the reconcile loop is going to scale the Control Plane.
	// Note: Refer to control plane contract for definition of scaling.
	// Note: IsScaling will be false if the Control Plane does not support replicas.
	IsScaling bool
}

// MachineDeploymentUpgradeTracker holds the current upgrade status of MachineDeployments.
type MachineDeploymentUpgradeTracker struct {
	// pendingCreateTopologyNames is the set of MachineDeployment topology names that are newly added to the
	// Cluster Topology but will not be created in the current reconcile loop.
	// By marking a MachineDeployment topology as pendingCreate we skip creating the MachineDeployment.
	// Nb. We use MachineDeployment topology names instead of MachineDeployment names because the new MachineDeployment
	// names can keep changing for each reconcile loop leading to continuous updates to the TopologyReconciled condition.
	pendingCreateTopologyNames sets.Set[string]

	// pendingUpgradeNames is the set of MachineDeployment names that are not going to pick up the new version
	// in the current reconcile loop.
	// By marking a MachineDeployment as pendingUpgrade we skip reconciling the MachineDeployment.
	pendingUpgradeNames sets.Set[string]

	// deferredNames is the set of MachineDeployment names that are not going to pick up the new version
	// in the current reconcile loop because they are deferred by the user.
	// Note: If a MachineDeployment is marked as deferred it should also be marked as pendingUpgrade.
	deferredNames sets.Set[string]

	// upgradingNames is the set of MachineDeployment names that are upgrading. This set contains the names of
	// MachineDeployments that are currently upgrading and the names of MachineDeployments that will pick up the upgrade
	// in the current reconcile loop.
	// Note: This information is used to:
	// - decide if ControlPlane can be upgraded.
	// - calculate MachineDeployment upgrade concurrency.
	// - update TopologyReconciled Condition.
	// - decide if the AfterClusterUpgrade hook can be called.
	upgradingNames sets.Set[string]

	// maxMachineDeploymentUpgradeConcurrency defines the maximum number of MachineDeployments that should be in an
	// upgrading state. This includes the MachineDeployments that are currently upgrading and the MachineDeployments that
	// will start the upgrade after the current reconcile loop.
	maxMachineDeploymentUpgradeConcurrency int
}

// UpgradeTrackerOptions contains the options for NewUpgradeTracker.
type UpgradeTrackerOptions struct {
	maxMDUpgradeConcurrency int
	maxMPUpgradeConcurrency int
}

// UpgradeTrackerOption returns an option for the NewUpgradeTracker function.
type UpgradeTrackerOption interface {
	ApplyToUpgradeTracker(options *UpgradeTrackerOptions)
}

// MaxMDUpgradeConcurrency sets the upper limit for the number of Machine Deployments that can upgrade
// concurrently.
type MaxMDUpgradeConcurrency int

// ApplyToUpgradeTracker applies the given UpgradeTrackerOptions.
func (m MaxMDUpgradeConcurrency) ApplyToUpgradeTracker(options *UpgradeTrackerOptions) {
	options.maxMDUpgradeConcurrency = int(m)
}

// MaxMPUpgradeConcurrency sets the upper limit for the number of Machine Pools that can upgrade
// concurrently.
type MaxMPUpgradeConcurrency int

// ApplyToUpgradeTracker applies the given UpgradeTrackerOptions.
func (m MaxMPUpgradeConcurrency) ApplyToUpgradeTracker(options *UpgradeTrackerOptions) {
	options.maxMPUpgradeConcurrency = int(m)
}

// NewUpgradeTracker returns an upgrade tracker with empty tracking information.
func NewUpgradeTracker(opts ...UpgradeTrackerOption) *UpgradeTracker {
	options := &UpgradeTrackerOptions{}
	for _, o := range opts {
		o.ApplyToUpgradeTracker(options)
	}
	if options.maxMDUpgradeConcurrency < 1 {
		// The concurrency should be at least 1.
		options.maxMDUpgradeConcurrency = 1
	}
	if options.maxMPUpgradeConcurrency < 1 {
		// The concurrency should be at least 1.
		options.maxMPUpgradeConcurrency = 1
	}
	return &UpgradeTracker{
		MachineDeployments: MachineDeploymentUpgradeTracker{
			pendingCreateTopologyNames:             sets.Set[string]{},
			pendingUpgradeNames:                    sets.Set[string]{},
			deferredNames:                          sets.Set[string]{},
			upgradingNames:                         sets.Set[string]{},
			maxMachineDeploymentUpgradeConcurrency: options.maxMDUpgradeConcurrency,
		},
		MachinePools: MachinePoolUpgradeTracker{
			pendingCreateTopologyNames:       sets.Set[string]{},
			pendingRollingOutNames:           sets.Set[string]{},
			deferredNames:                    sets.Set[string]{},
			rollingOutNames:                  sets.Set[string]{},
			maxMachinePoolRollOutConcurrency: options.maxMPUpgradeConcurrency,
		},
	}
}

// MarkUpgrading marks a MachineDeployment as currently upgrading or about to upgrade.
func (m *MachineDeploymentUpgradeTracker) MarkUpgrading(names ...string) {
	for _, name := range names {
		m.upgradingNames.Insert(name)
	}
}

// UpgradingNames returns the list of machine deployments that are upgrading or
// are about to upgrade.
func (m *MachineDeploymentUpgradeTracker) UpgradingNames() []string {
	return sets.List(m.upgradingNames)
}

// UpgradeConcurrencyReached returns true if the number of MachineDeployments upgrading is at the concurrency limit.
func (m *MachineDeploymentUpgradeTracker) UpgradeConcurrencyReached() bool {
	return m.upgradingNames.Len() >= m.maxMachineDeploymentUpgradeConcurrency
}

// MarkPendingCreate marks a machine deployment topology that is pending to be created.
// This is generally used to capture machine deployments that are yet to be created
// because the control plane is not yet stable.
func (m *MachineDeploymentUpgradeTracker) MarkPendingCreate(mdTopologyName string) {
	m.pendingCreateTopologyNames.Insert(mdTopologyName)
}

// IsPendingCreate returns true is the MachineDeployment topology is marked as pending create.
func (m *MachineDeploymentUpgradeTracker) IsPendingCreate(mdTopologyName string) bool {
	return m.pendingCreateTopologyNames.Has(mdTopologyName)
}

// IsAnyPendingCreate returns true if any of the machine deployments are pending
// to be created. Returns false, otherwise.
func (m *MachineDeploymentUpgradeTracker) IsAnyPendingCreate() bool {
	return len(m.pendingCreateTopologyNames) != 0
}

// PendingCreateTopologyNames returns the list of machine deployment topology names that
// are pending create.
func (m *MachineDeploymentUpgradeTracker) PendingCreateTopologyNames() []string {
	return sets.List(m.pendingCreateTopologyNames)
}

// MarkPendingUpgrade marks a machine deployment as in need of an upgrade.
// This is generally used to capture machine deployments that have not yet
// picked up the topology version.
func (m *MachineDeploymentUpgradeTracker) MarkPendingUpgrade(name string) {
	m.pendingUpgradeNames.Insert(name)
}

// IsPendingUpgrade returns true is the MachineDeployment marked as pending upgrade.
func (m *MachineDeploymentUpgradeTracker) IsPendingUpgrade(name string) bool {
	return m.pendingUpgradeNames.Has(name)
}

// IsAnyPendingUpgrade returns true if any of the machine deployments are pending
// an upgrade. Returns false, otherwise.
func (m *MachineDeploymentUpgradeTracker) IsAnyPendingUpgrade() bool {
	return len(m.pendingUpgradeNames) != 0
}

// PendingUpgradeNames returns the list of machine deployment names that
// are pending an upgrade.
func (m *MachineDeploymentUpgradeTracker) PendingUpgradeNames() []string {
	return sets.List(m.pendingUpgradeNames)
}

// MarkDeferredUpgrade marks that the upgrade for a MachineDeployment
// has been deferred.
func (m *MachineDeploymentUpgradeTracker) MarkDeferredUpgrade(name string) {
	m.deferredNames.Insert(name)
}

// DeferredUpgradeNames returns the list of MachineDeployment names for
// which the upgrade has been deferred.
func (m *MachineDeploymentUpgradeTracker) DeferredUpgradeNames() []string {
	return sets.List(m.deferredNames)
}

// DeferredUpgrade returns true if the upgrade has been deferred for any of the
// MachineDeployments. Returns false, otherwise.
func (m *MachineDeploymentUpgradeTracker) DeferredUpgrade() bool {
	return len(m.deferredNames) != 0
}

// MachinePoolUpgradeTracker holds the current upgrade status and makes upgrade
// decisions for MachinePools.
type MachinePoolUpgradeTracker struct {
	// pendingCreateTopologyNames is the set of MachinePool topology names that are newly added to the
	// Cluster Topology but will not be created in the current reconcile loop.
	// By marking a MachinePool topology as pendingCreate we skip creating the MachinePool.
	// Nb. We use MachinePool topology names instead of MachinePool names because the new MachinePool
	// names can keep changing for each reconcile loop leading to continuous updates to the TopologyReconciled condition.
	pendingCreateTopologyNames sets.Set[string]

	// pendingRollingOutNames is the set of MachinePool names that are not going to pick up the new version
	// in the current reconcile loop.
	// By marking a MachinePool as pendingRollingOut we skip reconciling the MachinePool.
	pendingRollingOutNames sets.Set[string]

	// deferredNames is the set of MachinePool names that are not going to pick up the new version
	// in the current reconcile loop because they are deferred by the user.
	// Note: If a MachinePool is marked as deferred it should also be marked as pendingUpgrade.
	deferredNames sets.Set[string]

	// rollingOutNames is the set of MachinePool names that are rolling out. This set contains the names of
	// MachinePools that are currently rolling out and the names of MachinePools that will pick up the upgrade
	// in the current reconcile loop.
	// Note: This information is used to:
	// - decide if ControlPlane can be upgraded.
	// - calculate MachinePool upgrade concurrency.
	// - update TopologyReconciled Condition.
	// - decide if the AfterClusterUpgrade hook can be called.
	rollingOutNames sets.Set[string]

	// maxMachinePoolRollOutConcurrency defines the maximum number of MachinePools that should be in an
	// rolling out state. This includes the MachinePools that are currently rolling out and the MachinePools that
	// will start the upgrade after the current reconcile loop.
	maxMachinePoolRollOutConcurrency int
}

// MarkRollingOut marks a MachinePool as currently rolling out or about to upgrade.
func (m *MachinePoolUpgradeTracker) MarkRollingOut(names ...string) {
	for _, name := range names {
		m.rollingOutNames.Insert(name)
	}
}

// RollingOutNames returns the list of machine pools that are rolling out or
// are about to roll out.
func (m *MachinePoolUpgradeTracker) RollingOutNames() []string {
	return sets.List(m.rollingOutNames)
}

// UpgradeConcurrencyReached returns true if the number of MachinePools rolling out is at the concurrency limit.
func (m *MachinePoolUpgradeTracker) UpgradeConcurrencyReached() bool {
	return m.rollingOutNames.Len() >= m.maxMachinePoolRollOutConcurrency
}

// MarkPendingCreate marks a machine pool topology that is pending to be created.
// This is generally used to capture machine pools that are yet to be created
// because the control plane is not yet stable.
func (m *MachinePoolUpgradeTracker) MarkPendingCreate(mdTopologyName string) {
	m.pendingCreateTopologyNames.Insert(mdTopologyName)
}

// IsPendingCreate returns true is the MachinePool topology is marked as pending create.
func (m *MachinePoolUpgradeTracker) IsPendingCreate(mdTopologyName string) bool {
	return m.pendingCreateTopologyNames.Has(mdTopologyName)
}

// IsAnyPendingCreate returns true if any of the machine pools are pending
// to be created. Returns false, otherwise.
func (m *MachinePoolUpgradeTracker) IsAnyPendingCreate() bool {
	return len(m.pendingCreateTopologyNames) != 0
}

// PendingCreateTopologyNames returns the list of machine pool topology names that
// are pending create.
func (m *MachinePoolUpgradeTracker) PendingCreateTopologyNames() []string {
	return sets.List(m.pendingCreateTopologyNames)
}

// MarkPendingUpgrade marks a machine pool as in need of an upgrade.
// This is generally used to capture machine pools that have not yet
// picked up the topology version.
func (m *MachinePoolUpgradeTracker) MarkPendingUpgrade(name string) {
	m.pendingRollingOutNames.Insert(name)
}

// IsPendingUpgrade returns true is the MachinePool marked as pending upgrade.
func (m *MachinePoolUpgradeTracker) IsPendingUpgrade(name string) bool {
	return m.pendingRollingOutNames.Has(name)
}

// IsAnyPendingUpgrade returns true if any of the machine pools are pending
// an upgrade. Returns false, otherwise.
func (m *MachinePoolUpgradeTracker) IsAnyPendingUpgrade() bool {
	return len(m.pendingRollingOutNames) != 0
}

// PendingUpgradeNames returns the list of machine pool names that
// are pending an upgrade.
func (m *MachinePoolUpgradeTracker) PendingUpgradeNames() []string {
	return sets.List(m.pendingRollingOutNames)
}

// MarkDeferredUpgrade marks that the upgrade for a MachinePool
// has been deferred.
func (m *MachinePoolUpgradeTracker) MarkDeferredUpgrade(name string) {
	m.deferredNames.Insert(name)
}

// DeferredUpgradeNames returns the list of MachinePool names for
// which the upgrade has been deferred.
func (m *MachinePoolUpgradeTracker) DeferredUpgradeNames() []string {
	return sets.List(m.deferredNames)
}

// DeferredUpgrade returns true if the upgrade has been deferred for any of the
// MachinePools. Returns false, otherwise.
func (m *MachinePoolUpgradeTracker) DeferredUpgrade() bool {
	return len(m.deferredNames) != 0
}
