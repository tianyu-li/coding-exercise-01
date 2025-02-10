SELECT 
  _id as user_id,
  createdDate as created_date, 
  lastLogin as last_login,
  role, 
  state,
  active,
  signUpSource as signup_source
 FROM `tli-sample-01.fetch.users`