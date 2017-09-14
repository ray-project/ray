

for i in 10, 20, 50:
do
for step in 0.0001, 0.00005, 0.00001:
do
	 python train.py --alg A2C --env PongDeterministic-v4 --upload-dir "file:///tmp/ray/step_${step}/nstep_${i}" --config '{"num_workers": 24, "batch_size": '"${i}"', "policy_config": { "vf_coeff": 0.5, "entropy_coeff": 0.01, "grad_clip": 40.0, "step_size": '"${step}"' }}'
done
done