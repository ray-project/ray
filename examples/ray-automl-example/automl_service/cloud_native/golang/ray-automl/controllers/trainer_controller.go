/*
Copyright 2023.

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

package controllers

import (
	"context"
	"github.com/go-logr/logr"
	automlv1 "github.com/ray-automl/apis/automl/v1"
	"github.com/ray-automl/common"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// TrainerReconciler reconciles a Trainer object
type TrainerReconciler struct {
	client.Client
	Config *rest.Config
	Log    logr.Logger
	Scheme *runtime.Scheme
}

var _ reconcile.Reconciler = &TrainerReconciler{}

//+kubebuilder:rbac:groups=automl.my.domain,resources=trainers,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=automl.my.domain,resources=trainers/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=automl.my.domain,resources=trainers/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// the Trainer object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.13.0/pkg/reconcile
func (r *TrainerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var err error

	// Try to fetch the Trainer instance
	instance := &automlv1.Trainer{}
	if err = r.Get(context.TODO(), req.NamespacedName, instance); err == nil {
		r.Log.Info("reconcile for instance", "req", req.NamespacedName)
		return r.trainerReconcile(req, instance)
	}

	// No match found
	if errors.IsNotFound(err) {
		r.Log.Info("Read request instance not found error!", "name", req.NamespacedName)
	} else {
		r.Log.Error(err, "Read request instance error!")
	}

	return ctrl.Result{}, nil
}

func (r *TrainerReconciler) trainerReconcile(req ctrl.Request, instance *automlv1.Trainer) (ctrl.Result, error) {
	var err error

	if err = r.reconcileServices(instance); err != nil && !errors.IsAlreadyExists(err) {
		r.Log.Error(err, "failed to create service for instance", "instance", req.NamespacedName)
		return ctrl.Result{Requeue: true}, nil
	}

	if err := r.reconcileTrainerDeploy(instance); err != nil && !errors.IsAlreadyExists(err) {
		r.Log.Error(err, "failed to create trainer deploy for instance", "instance", req.NamespacedName)
		return ctrl.Result{Requeue: true}, nil
	}

	return ctrl.Result{}, nil
}

func (r *TrainerReconciler) reconcileServices(instance *automlv1.Trainer) error {
	service := common.NewService(instance, r.Log)
	common.SetTrainerOwnerReference(service, instance)
	if err := r.Create(context.TODO(), service); err != nil {
		return err
	}
	return nil
}

func (r *TrainerReconciler) reconcileTrainerDeploy(instance *automlv1.Trainer) error {
	deployment := common.NewDeployment(instance, r.Log)
	common.SetTrainerOwnerReference(deployment, instance)
	if err := r.Create(context.TODO(), deployment); err != nil {
		return err
	}
	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *TrainerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&automlv1.Trainer{}).
		Complete(r)
}
