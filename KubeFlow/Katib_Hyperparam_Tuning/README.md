### Hyperparameter Tuning with Katib

#### launch Experiement

```
# launch experiment
kubectl apply -f experiment-distributed.yaml
# get experiment
kubectl get experiment -n <NAMESPACE>
# describe experiment to debug
kubectl describe experiment -n <NAMESPACE> fashion-mnist-experiment-distributed-1 
# check the TFJob launched as part of hyper-parameter tuning 
kubectl get tfjob -n <NAMESPACE>
kubectl describe tfjob <TFJOB> -n kubeflow
# check logs 
kubectl logs -f <TJFOB_NAME>-chief-0 -n kubeflow
```

Katib supports multiple optimization algorithms such as random search, grid search, bayesian optimization and hyperband


Example output of hyperparameter tuning in KubeFlow UI, can drill down into each job

![alt text](images/hyper.png?raw=true)
